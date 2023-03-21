import asyncio
import typing as T
from datetime import datetime
from pathlib import Path
from copy import copy

import cloudpickle

from ..base import ExecutorObj
from .utils import (
    JobStatusAttr, JobEmitError, InvalidStateError, JobStatusType,
)
from .condition import Condition
from ..middle.capture import CaptureOut
from ..middle.dir import ChDir
from ..log import logger


if T.TYPE_CHECKING:
    from ..core import Engine


def get_callable_name(callable) -> str:
    if hasattr(callable, "func"):
        inner_func = getattr(callable, "func")
        return get_callable_name(inner_func)
    elif hasattr(callable, "__name__"):
        return getattr(callable, "__name__")
    elif hasattr(callable, "__class__"):
        return getattr(callable, "__class__").__name__
    else:
        return str(callable)


class Job(ExecutorObj):

    status = JobStatusAttr()

    def __init__(
            self,
            func: T.Callable,
            args: T.Optional[tuple] = None,
            kwargs: T.Optional[dict] = None,
            callback: T.Optional[T.Callable[[T.Any], None]] = None,
            error_callback: T.Optional[T.Callable[[Exception], None]] = None,
            print_traceback: bool = True,
            retries: int = 0,
            retry_time_delta: float = 0.0,
            name: T.Optional[str] = None,
            condition: T.Optional[Condition] = None,
            wait_time_delta: float = 0.01,
            redirect_out_err: bool = False,
            change_dir: bool = False,
            **attrs
            ) -> None:
        super().__init__()
        self.func = func
        self.args = args or tuple()
        self.kwargs = kwargs or {}
        self.callback = callback
        self.error_callback = error_callback
        self.print_traceback = print_traceback
        self.retries = retries
        self.retry_count = 0
        self.retry_time_delta = retry_time_delta
        self.engine: T.Optional["Engine"] = None
        self._status: str = "created"
        self.name = name or func.__name__
        self.attrs = attrs
        self.task: T.Optional[asyncio.Task] = None
        self.condition = condition
        self.wait_time_delta = wait_time_delta
        self.redirect_out_err = redirect_out_err
        self.change_dir = change_dir
        self.created_time: datetime = datetime.now()
        self.submit_time: T.Optional[datetime] = None
        self.stoped_time: T.Optional[datetime] = None
        self._executor: T.Any = None
        self._exception: T.Optional[Exception] = None

    def __repr__(self) -> str:
        func_name = get_callable_name(self.func)
        attrs = [
            f"status={self.status}",
            f"id={self.id}",
            f"func={func_name}",
        ]
        if self.condition:
            attrs.append(f" condition={repr(self.condition)}")
        attr_str = " ".join(attrs)
        return f"<{self.__class__.__name__} {attr_str}>"

    def __str__(self) -> str:
        return repr(self)

    def has_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            return self.engine.resource.n_job > 0

    def consume_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.resource.n_job -= 1
            return True

    def release_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.resource.n_job += 1
            return True

    def runnable(self) -> bool:
        if self.engine is None:
            return False
        if self.condition is not None:
            return self.condition.satisfy(self.engine) and self.has_resource()
        else:
            return self.has_resource()

    async def emit(self) -> asyncio.Task:
        logger.info(f"Emit job {self}, watting for run.")
        if self.status != 'pending':
            raise JobEmitError(
                f"{self} is not in valid status(pending)")
        self.submit_time = datetime.now()
        loop = asyncio.get_running_loop()
        task = loop.create_task(self.wait_and_run())
        self.task = task
        return task

    def process_func(self):
        cache_dir = self.cache_dir.resolve()
        if self.redirect_out_err and (not isinstance(self.func, CaptureOut)):
            path_stdout = cache_dir / 'stdout.txt'
            path_stderr = cache_dir / 'stderr.txt'
            self.func = CaptureOut(self.func, path_stdout, path_stderr)
        if self.change_dir:
            self.func = ChDir(self.func, cache_dir)

    async def wait_and_run(self):
        while True:
            if self.runnable() and self.consume_resource():
                logger.info(f"Start run job {self}")
                self.process_func()
                self.status = "running"
                try:
                    res = await self.run()
                    await self.on_done(res)
                    return res
                except Exception as e:
                    await self.on_failed(e)
                    break
            else:
                await asyncio.sleep(self.wait_time_delta)

    async def run(self):
        pass

    async def rerun(self):
        _valid_status = ("canceled", "done", "failed")
        if self.status not in _valid_status:
            raise JobEmitError(
                f"{self} is not in valid status({_valid_status})")
        logger.info(f"Rerun job {self}")
        self.status = "pending"
        await self.emit()

    async def _on_finish(self, new_status: JobStatusType = "done"):
        self.status = new_status
        self.release_resource()

    async def on_done(self, res):
        logger.info(f"Job {self} done.")
        if self.callback is not None:
            self.callback(res)
        await self._on_finish("done")

    async def on_failed(self, e: Exception):
        logger.error(f"Job {self} failed: {repr(e)}")
        if self.print_traceback:
            logger.exception(e)
        self._exception = e
        if self.error_callback is not None:
            self.error_callback(e)
        await self._on_finish("failed")
        if self.retry_count < self.retries:
            self.retry_count += 1
            await asyncio.sleep(self.retry_time_delta)
            await self.rerun()

    async def cancel(self):
        logger.info(f"Cancel job {self}.")
        self.task.cancel()
        if self.status == "running":
            try:
                self.clear_context()
            except Exception as e:  # pragma: no cover
                print(str(e))
            finally:
                await self._on_finish("canceled")
        elif self.status == "pending":
            self.status = "canceled"

    def clear_context(self):
        pass

    def result(self) -> T.Any:
        if self.task is not None:
            if self.status != "done":
                raise InvalidStateError(f"{self} is not done.")
            return self.task.result()
        else:
            raise InvalidStateError(f"{self} is not emitted.")

    def exception(self):
        return self._exception

    def serialization(self) -> bytes:
        job = copy(self)
        job.task = None
        job.engine = None
        job._executor = None
        bytes_ = cloudpickle.dumps(job)
        return bytes_

    @staticmethod
    def deserialization(bytes_: bytes) -> "Job":
        job: "Job" = cloudpickle.loads(bytes_)
        return job

    async def join(self, timeout: T.Optional[float] = None):
        if self.task is None:
            raise InvalidStateError(f"{self} is not emitted.")
        await asyncio.wait([self.task], timeout=timeout)

    @property
    def cache_dir(self) -> T.Optional[Path]:
        if self.engine is None:
            return None
        parent = self.engine.cache_dir
        path = parent / self.id
        path.mkdir(parents=True, exist_ok=True)
        return path
