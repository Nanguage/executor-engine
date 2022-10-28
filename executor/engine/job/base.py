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
from .capture import CaptureOut


if T.TYPE_CHECKING:
    from ..core import Engine


class Job(ExecutorObj):

    job_type: str = "base"

    status = JobStatusAttr()

    def __init__(
            self,
            func: T.Callable,
            args: T.Optional[tuple] = None,
            kwargs: T.Optional[dict] = None,
            callback: T.Optional[T.Callable[[T.Any], None]] = None,
            error_callback: T.Optional[T.Callable[[Exception], None]] = None,
            name: T.Optional[str] = None,
            condition: T.Optional[Condition] = None,
            time_delta: float = 0.01,
            redirect_out_err: bool = False,
            **attrs
            ) -> None:
        super().__init__()
        self.func = func
        self.args = args or tuple()
        self.kwargs = kwargs or {}
        self.callback = callback
        self.error_callback = error_callback
        self.engine: T.Optional["Engine"] = None
        self._status: str = "created"
        self.name = name or func.__name__
        self.attrs = attrs
        self.task: T.Optional[asyncio.Task] = None
        self.condition = condition
        if self.condition is not None:
            self.condition.job = self
        self.time_delta = time_delta
        self.redirect_out_err = redirect_out_err
        self.created_time: datetime = datetime.now()
        self.submit_time: T.Optional[datetime] = None
        self.stoped_time: T.Optional[datetime] = None
        self.executor = None

    def __repr__(self) -> str:
        attrs = [
            f"status={self.status}",
            f"id={self.id}",
            f"func={self.func}",
        ]
        if self.condition:
            attrs.append(f" condition={self.condition}")
        attr_str = " ".join(attrs)
        return f"<{self.__class__.__name__} {attr_str}/>"

    def __str__(self) -> str:
        return repr(self)

    def has_resource(self) -> bool:
        return True

    def consume_resource(self) -> bool:
        return True

    def release_resource(self) -> bool:
        return True

    def runnable(self) -> bool:
        if self.condition is not None:
            return self.condition.satisfy() and self.has_resource()
        else:
            return self.has_resource()

    async def emit(self) -> asyncio.Task:
        if self.status != 'pending':
            raise JobEmitError(
                f"{self} is not in valid status(pending)")
        loop = asyncio.get_running_loop()
        task = loop.create_task(self.wait_and_run())
        self.task = task
        return task

    async def wait_and_run(self):
        while True:
            if self.runnable() and self.consume_resource():
                self.status = "running"
                if self.redirect_out_err:
                    path_stdout = self.cache_dir / 'stdout.txt'
                    path_stderr = self.cache_dir / 'stderr.txt'
                    self.func = CaptureOut(self.func, path_stdout, path_stderr)
                    res = await self.run()
                else:
                    res = await self.run()
                return res
            else:
                await asyncio.sleep(self.time_delta)

    async def run(self):
        pass

    async def rerun(self):
        _valid_status = ("canceled", "done", "failed")
        if self.status not in _valid_status:
            raise JobEmitError(
                f"{self} is not in valid status({_valid_status})")
        await self.engine.submit(self)

    async def _on_finish(self, new_status: JobStatusType = "done"):
        self.status = new_status
        self.release_resource()

    async def on_done(self, res):
        if self.callback is not None:
            self.callback(res)
        await self._on_finish("done")

    async def on_failed(self, e: Exception):
        if self.error_callback is not None:
            self.error_callback(e)
        await self._on_finish("failed")

    async def cancel(self):
        self.task.cancel()
        if self.status == "running":
            try:
                self.clear_context()
            except Exception as e:
                print(str(e))
            finally:
                await self._on_finish("canceled")
        elif self.status == "pending":
            self.engine.jobs.pending.pop(self.id)

    def clear_context(self):
        pass

    def result(self) -> T.Any:
        if self.status != "done":
            raise InvalidStateError(f"{self} is not done.")
        if self.task is not None:
            return self.task.result()
        else:
            raise InvalidStateError(f"{self} is not emitted.")

    def to_dict(self) -> dict:
        if self.condition is not None:
            cond = self.condition.to_dict()
        else:
            cond = None

        return {
            'id': self.id,
            'name': self.name,
            'args': self.args,
            'kwargs': self.kwargs,
            'condition': cond,
            'status': self.status,
            'job_type': self.job_type,
            'check_time': datetime.now(),
            'created_time': self.created_time,
            'submit_time': self.submit_time,
            'stoped_time': self.stoped_time,
        }

    def serialization(self) -> bytes:
        job = copy(self)
        job.task = None
        job.engine = None
        job.executor = None
        if job.condition is not None:
            condition = copy(job.condition)
            condition.job = job
            job.condition = condition
        try:
            bytes_ = cloudpickle.dumps(job)
        except:
            import ipdb; ipdb.set_trace()
        return bytes_

    @staticmethod
    def deserialization(bytes_: bytes) -> "Job":
        job: "Job" = cloudpickle.loads(bytes_)
        return job

    async def join(self):
        if self.task is None:
            raise InvalidStateError(f"{self} is not emitted.")
        await self.task

    @property
    def cache_dir(self) -> T.Optional[Path]:
        if self.engine is None:
            return None
        parent = self.engine.cache_dir
        path = parent / self.id
        path.mkdir(parents=True, exist_ok=True)
        return path
