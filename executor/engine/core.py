import typing as T
from dataclasses import dataclass
from pathlib import Path
import asyncio

from .base import ExecutorObj
from .job.base import Job
from .manager import Jobs

if T.TYPE_CHECKING:
    from dask.distributed import Client


@dataclass
class EngineSetting:
    max_threads: T.Optional[int] = None
    max_processes: T.Optional[int] = None
    max_dask_jobs: T.Optional[int] = None
    max_jobs: T.Optional[int] = 20
    cache_path: T.Optional[str] = None


@dataclass
class Resource:
    n_thread: T.Union[int, float]
    n_process: T.Union[int, float]
    n_dask: T.Union[int, float]
    n_job: T.Union[int, float]


class Engine(ExecutorObj):
    def __init__(
            self,
            setting: T.Optional[EngineSetting] = None,
            jobs: T.Optional[Jobs] = None,
            ) -> None:
        super().__init__()
        if setting is None:
            setting = EngineSetting()
        self.setting = setting
        self.setup_by_setting()
        if jobs is None:
            jobs = Jobs(self.cache_dir / "jobs")
        self.jobs: Jobs = jobs
        self._dask_client = None

    def setup_by_setting(self):
        setting = self.setting
        self.resource = Resource(
            n_thread=setting.max_threads or float('inf'),
            n_process=setting.max_processes or float('inf'),
            n_dask=setting.max_dask_jobs or float('inf'),
            n_job=setting.max_jobs or float('inf'),
        )
        self.cache_dir = self.get_cache_dir()

    async def submit(self, job: Job):
        if job.status == "created":
            job.engine = self
            job._status = "pending"
            self.jobs.add(job)
        else:
            job.status = "pending"
        await job.emit()

    async def remove(self, job: Job):
        if job.status in ('running', 'pending'):
            await job.cancel()
        self.jobs.remove(job)

    async def wait(self, timeout: T.Optional[float] = None):
        """Block until all jobs are finished or timeout."""
        tasks = []
        for job in self.jobs.all_jobs():
            if job.task is not None:
                tasks.append(job.task)
        if tasks:
            await asyncio.wait(tasks, timeout=timeout)

    def get_cache_dir(self) -> Path:
        cache_path = self.setting.cache_path
        if cache_path is not None:
            path = cache_path
        else:
            path = f".executor/{self.id}"
        path_obj = Path(path)
        path_obj.mkdir(parents=True, exist_ok=True)
        return path_obj

    async def cancel_all(self):
        for job in self.jobs.running.values():
            await job.cancel()

    @property
    def dask_client(self):
        from .job.dask.manager import get_default_client
        if self._dask_client is None:
            self._dask_client = get_default_client()
        return self._dask_client

    @dask_client.setter
    def dask_client(self, client: "Client"):
        if not client.asynchronous:
            raise ValueError("Dask client must be asynchronous.")
        self._dask_client = client
