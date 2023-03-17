import typing as T
import inspect
from concurrent.futures import ThreadPoolExecutor, Future
import functools
from copy import copy

from funcdesc import parse_func

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


class TaskBase(object):
    def __init__(
            self, target_func: T.Callable,
            engine: 'Engine',
            job_type: JOB_TYPES = 'process',
            name: T.Optional[str] = None,
            description: T.Optional[str] = None,
            tags: T.Optional[T.List] = None,
            **job_attrs):
        self.engine = engine
        self.target_func = target_func
        self.__signature__ = inspect.signature(target_func)
        self.job_type = job_type
        self.desc = parse_func(target_func)
        self.name = name or target_func.__name__
        self.description = description or self.target_func.__doc__
        self.tags = tags or []
        self.job_attrs = job_attrs

    def create_job(self, args, kwargs, **attrs) -> 'Job':
        job_class = job_type_classes[self.job_type]
        job_attrs = copy(self.job_attrs)
        job_attrs.update(attrs)
        job = job_class(
            self.target_func, args, kwargs,
            **job_attrs
        )
        return job

    async def submit_and_wait(self, job: 'Job') -> T.Any:
        await self.engine.submit(job)
        await job.join()
        return job.result()


class SyncTask(TaskBase):

    pool = ThreadPoolExecutor()

    @property
    def async_mode(self):
        return False

    def resolve_args(
            self, args: list, kwargs: dict
            ) -> T.Tuple[list, dict]:
        new_args = []
        for arg in args:
            if isinstance(arg, Future):
                new_args.append(arg.result())
            else:
                new_args.append(arg)
        new_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, Future):
                new_kwargs[k] = v.result()
            else:
                new_kwargs[k] = v
        return new_args, new_kwargs

    def run_in_pool(self, args: list, kwargs: dict) -> T.Any:
        args, kwargs = self.resolve_args(args, kwargs)
        job = self.create_job(args, kwargs)
        with event_loop() as loop:
            coro = self.submit_and_wait(job)
            res = loop.run_until_complete(coro)
        return res

    def submit(self, *args, **kwargs) -> 'Future':
        fut = self.pool.submit(self.run_in_pool, args, kwargs)
        return fut

    def __call__(self, *args, **kwargs) -> T.Any:
        fut = self.submit(*args, **kwargs)
        return fut.result()

    def to_async(self) -> "AsyncTask":
        return AsyncTask(
            self.target_func, self.engine, self.job_type,
            self.name, self.tags, **self.job_attrs,
        )


class AsyncTask(TaskBase):
    @property
    def async_mode(self):
        return True

    async def submit(self, *args, **kwargs):
        job = self.create_job(args, kwargs)
        await self.engine.submit(job)
        return job

    async def __call__(self, *args, **kwargs) -> T.Any:
        job = self.create_job(args, kwargs)
        return await self.submit_and_wait(job)

    def to_sync(self) -> "SyncTask":
        return SyncTask(
            self.target_func, self.engine, self.job_type,
            self.name, self.tags, **self.job_attrs,
        )


def task(
        func=None,
        engine: T.Optional['Engine'] = None,
        async_mode: bool = False,
        job_type: JOB_TYPES = 'process',
        name: T.Optional[str] = None,
        tags: T.Optional[T.List] = None,
        **job_attrs):
    if func is None:
        return functools.partial(
            task, engine=engine, async_mode=async_mode,
            job_type=job_type,
            name=name, tags=tags,
        )
    if engine is None:
        engine = get_engine()

    if async_mode:
        task_cls = AsyncTask
    else:
        task_cls = SyncTask

    return task_cls(
        func, engine, job_type,
        name, tags, **job_attrs,
    )
