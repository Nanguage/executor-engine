import typing as T
import inspect
import functools
from copy import copy

from funcdesc import parse_func

from ..core import Engine
from ..job import Job, LocalJob, ThreadJob, ProcessJob
from ..job.extend import SubprocessJob, WebAppJob


job_type_classes: T.Dict[str, T.Type[Job]] = {
    'local': LocalJob,
    'thread': ThreadJob,
    'process': ProcessJob,
    'subprocess': SubprocessJob,
    'webapp': WebAppJob,
}


JOB_TYPES = T.Literal['local', 'thread', 'process', 'subprocess', 'webapp']


_engine: T.Optional[Engine] = None


def get_default_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = Engine()
    return _engine


def set_default_engine(engine: Engine):
    global _engine
    _engine = engine


class LauncherBase(object):
    def __init__(
            self, target_func: T.Callable,
            engine: T.Optional['Engine'] = None,
            job_type: JOB_TYPES = 'process',
            name: T.Optional[str] = None,
            description: T.Optional[str] = None,
            tags: T.Optional[T.List[str]] = None,
            **job_attrs):
        self._engine = engine
        self.target_func = target_func
        self.__signature__ = inspect.signature(target_func)
        self.job_type = job_type
        self.desc = parse_func(target_func)
        self.name = name or target_func.__name__
        self.description = description or self.target_func.__doc__
        self.tags = tags or []
        self.job_attrs = job_attrs

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = get_default_engine()
            if (self._engine._loop_thread is None) or \
               (not self._engine._loop_thread.is_alive()):
                self._engine.start()
        return self._engine

    def create_job(self, args, kwargs, **attrs) -> 'Job':
        job_class = job_type_classes[self.job_type]
        job_attrs = copy(self.job_attrs)
        job_attrs.update(attrs)
        job = job_class(
            self.target_func, args, kwargs,
            **job_attrs
        )
        return job

    @staticmethod
    def _fetch_result(job: 'Job') -> T.Any:
        if job.status == "failed":
            raise job.exception()
        elif job.status == "cancelled":
            raise RuntimeError("Job cancelled")
        else:
            return job.result()


class SyncLauncher(LauncherBase):

    @property
    def async_mode(self):
        return False

    def submit(self, *args, **kwargs) -> Job:
        job = self.create_job(args, kwargs)
        self.engine.submit(job).result()
        return job

    def __call__(self, *args, **kwargs) -> T.Any:
        job = self.submit(*args, **kwargs)
        self.engine.wait_job(job)
        return self._fetch_result(job)

    def to_async(self) -> "AsyncLauncher":
        return AsyncLauncher(
            self.target_func, self.engine, self.job_type,
            self.name, self.description, self.tags, **self.job_attrs,
        )


class AsyncLauncher(LauncherBase):
    @property
    def async_mode(self):
        return True

    async def submit(self, *args, **kwargs):
        job = self.create_job(args, kwargs)
        await self.engine.submit_async(job)
        return job

    async def __call__(self, *args, **kwargs) -> T.Any:
        job = await self.submit(*args, **kwargs)
        await job.join()
        return self._fetch_result(job)

    def to_sync(self) -> "SyncLauncher":
        return SyncLauncher(
            self.target_func, self.engine, self.job_type,
            self.name, self.description, self.tags, **self.job_attrs,
        )


def launcher(
        func=None,
        engine: T.Optional['Engine'] = None,
        async_mode: bool = False,
        job_type: JOB_TYPES = 'process',
        name: T.Optional[str] = None,
        description: T.Optional[str] = None,
        tags: T.Optional[T.List[str]] = None,
        **job_attrs):
    if func is None:
        return functools.partial(
            launcher, engine=engine, async_mode=async_mode,
            job_type=job_type,
            name=name, tags=tags,
        )

    launcher_cls: T.Union[T.Type[AsyncLauncher], T.Type[SyncLauncher]]
    if async_mode:
        launcher_cls = AsyncLauncher
    else:
        launcher_cls = SyncLauncher

    return launcher_cls(
        func, engine, job_type,
        name, description, tags, **job_attrs,
    )
