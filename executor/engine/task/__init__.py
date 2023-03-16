import typing as T
import inspect
from concurrent.futures import ThreadPoolExecutor, Future
import functools

from ..core import Engine
from ..job import Job, LocalJob, ThreadJob, ProcessJob
from ..job.extend import SubprocessJob, WebAppJob
from ..utils import event_loop


job_type_classes: T.Dict[str, T.Type[Job]] = {
    'local': LocalJob,
    'thread': ThreadJob,
    'process': ProcessJob,
    'subprocess': SubprocessJob,
    'webapp': WebAppJob,
}


JOB_TYPES = T.Literal['local', 'thread', 'process', 'subprocess', 'webapp']


_engine = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = Engine()
    return _engine


class Task(object):

    pool = ThreadPoolExecutor()

    def __init__(
            self, target_func: T.Callable,
            engine: 'Engine',
            job_type: JOB_TYPES = 'process'):
        self.engine = engine
        self.target_func = target_func
        self.__signature__ = inspect.signature(target_func)
        self.job_type = job_type

    def create_job(self, *args, **kwargs) -> 'Job':
        job_class = job_type_classes[self.job_type]
        job = job_class(self.target_func, args, kwargs)
        return job

    async def submit_and_wait(self, job: 'Job') -> T.Any:
        await self.engine.submit(job)
        await job.join()
        return job.result()

    def run_in_pool(self, job) -> T.Any:
        with event_loop() as loop:
            coro = self.submit_and_wait(job)
            res = loop.run_until_complete(coro)
        return res

    def submit(self, *args, **kwargs) -> 'JobFuture':
        job = self.create_job(*args, **kwargs)
        fut = self.pool.submit(self.run_in_pool, job)
        return JobFuture(fut, job)


def task(
        func=None,
        engine: T.Optional['Engine'] = None,
        job_type: JOB_TYPES = 'process'):
    if func is None:
        return functools.partial(
            task, engine=engine, job_type=job_type)
    if engine is None:
        engine = get_engine()
    return Task(func, engine, job_type)


class JobFuture():
    """Wrap concurrent.futures.Future"""
    def __init__(self, fut: Future, job: Job):
        self.fut = fut
        self.job = job

    def add_done_callback(self, fn: T.Callable[[Future], None]) -> None:
        self.fut.add_done_callback(fn)

    def cancel(self) -> bool:
        return self.fut.cancel()

    def cancelled(self) -> bool:
        return self.fut.cancelled()

    def done(self) -> bool:
        return self.fut.done()

    def exception(self, timeout: T.Optional[float] = None) -> T.Optional[BaseException]:
        return self.fut.exception(timeout)

    def result(self) -> T.Any:
        return self.fut.result()

    def running(self) -> bool:
        return self.fut.running()
    
    def set_exception(self, exception: Exception) -> None:
        self.fut.set_exception(exception)

    def set_result(self, result: T.Any) -> None:
        self.fut.set_result(result)
