import asyncio
import typing as T
from datetime import datetime

from ..utils import CheckAttrRange
from ..error import ExecutorError
from ..base import ExecutorObj


if T.TYPE_CHECKING:
    from ..engine import Engine


JobStatusType = T.Literal['pending', 'running', 'failed', 'done', 'canceled']
valid_job_statuses = JobStatusType.__args__  # type: ignore


class JobStatusAttr(CheckAttrRange):
    valid_range: T.Iterable[JobStatusType] = valid_job_statuses
    attr = "_status"

    def __set__(self, obj: "Job", value: JobStatusType):
        self.check(obj, value)
        if obj.engine is not None:
            obj.engine.jobs.move_job_store(obj, value)
        setattr(obj, self.attr, value)


class JobEmitError(ExecutorError):
    pass


class InvalidStateError(ExecutorError):
    pass


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
            **attrs
            ) -> None:
        super().__init__()
        self.func = func
        self.args = args or tuple()
        self.kwargs = kwargs or {}
        self.callback = callback
        self.error_callback = error_callback
        self.engine: T.Optional["Engine"] = None
        self._status: str = "pending"
        self.name = name or func.__name__
        self.attrs = attrs
        self.task: T.Optional[asyncio.Task] = None

    def __repr__(self) -> str:
        return f"<Job status={self.status} id={self.id[-8:]} func={self.func}>"

    def has_resource(self) -> bool:
        return True

    def consume_resource(self) -> bool:
        return True

    def release_resource(self) -> bool:
        return True

    async def emit(self) -> asyncio.Task:
        _valid_status = ("pending", "canceled", "done", "failed")
        if self.status not in _valid_status:
            raise JobEmitError(
                f"{self} is not in valid status({_valid_status})")
        self.status = "running"
        loop = asyncio.get_running_loop()
        task = loop.create_task(self.run())
        self.task = task
        return task

    async def _on_finish(self, new_status: JobStatusType = "done"):
        self.status = new_status
        self.release_resource()
        if self.engine is not None:
            await self.engine.activate()

    async def on_done(self, res):
        if self.callback is not None:
            self.callback(res)
        await self._on_finish("done")

    async def on_failed(self, e: Exception):
        if self.error_callback is not None:
            self.error_callback(e)
        await self._on_finish("failed")

    async def run(self):
        pass

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

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'status': self.status,
            'check_time': str(datetime.now()),
            'job_type': self.job_type,
        }

    async def join(self):
        if self.task is None:
            raise InvalidStateError(f"{self} is not emitted.")
        await self.task


class LocalJob(Job):
    job_type = "local"

    async def run(self):
        success = False
        try:
            res = self.func(*self.args, **self.kwargs)
            success = True
        except Exception as e:
            await self.on_failed(e)
        if success:
            await self.on_done(res)
        return res
