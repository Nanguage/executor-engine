import typing as T
import asyncio
from datetime import datetime
from concurrent.futures import Future

from ..utils import CheckAttrRange, ExecutorError


if T.TYPE_CHECKING:
    from .base import Job


JobStatusType = T.Literal['pending', 'running', 'failed', 'done', 'cancelled']
valid_job_statuses: T.List[JobStatusType] = [
    'pending', 'running', 'failed', 'done', 'cancelled']


class JobStatusAttr(CheckAttrRange):
    valid_range: T.Iterable[JobStatusType] = valid_job_statuses
    attr = "_status"

    def __set__(self, obj: "Job", value: JobStatusType):
        self.check(obj, value)
        old_status = getattr(obj, self.attr)
        setattr(obj, self.attr, value)
        if obj.engine is not None:
            obj.engine.jobs.move_job_store(
                obj, value, old_status)
        if value in ('done', 'failed', 'cancelled'):
            obj.stoped_time = datetime.now()


class InvalidStateError(ExecutorError):
    def __init__(self, job: "Job", valid_status: T.List[JobStatusType]):
        self.valid_status = valid_status
        super().__init__(
            f"Invalid state: {job} is in {job.status} state, "
            f"but should be in {valid_status} state.")


_T = T.TypeVar("_T")


def _gen_initializer(gen_func, args=tuple(), kwargs={}):  # pragma: no cover
    global _generator
    _generator = gen_func(*args, **kwargs)


def _gen_next(fut: T.Optional[Future] = None):  # pragma: no cover
    global _generator
    if fut is None:
        return next(_generator)
    else:
        return next(fut)


def _gen_anext(fut: T.Optional[Future] = None):  # pragma: no cover
    global _generator
    if fut is None:
        return asyncio.run(_generator.__anext__())
    else:
        return asyncio.run(fut.__anext__())


class GeneratorWrapper(T.Generic[_T]):
    """
    wrap a generator in executor pool
    """
    def __init__(self, job: "Job", fut: T.Optional[Future] = None):
        self._job = job
        self._fut = fut

    def __iter__(self):
        return self

    def __next__(self) -> _T:
        try:
            return self._job._executor.submit(_gen_next, self._fut).result()
        except Exception as e:
            engine = self._job.engine
            if engine is None:
                loop = asyncio.get_event_loop()
            else:
                loop = engine.loop
            if isinstance(e, StopIteration):
                cor = self._job.on_done(self)
            else:
                cor = self._job.on_failed(e)
            fut = asyncio.run_coroutine_threadsafe(cor, loop)
            fut.result()
            raise e

    def __aiter__(self):
        return self

    async def __anext__(self) -> _T:
        try:
            fut = self._job._executor.submit(_gen_anext, self._fut)
            res = await asyncio.wrap_future(fut)
            return res
        except Exception as e:
            if isinstance(e, StopAsyncIteration):
                await self._job.on_done(self)
            else:
                await self._job.on_failed(e)
            raise e
