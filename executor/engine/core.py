import typing as T
from dataclasses import dataclass
from pathlib import Path
import asyncio
from threading import Thread
import time
import concurrent.futures

from .base import ExecutorObj
from .job.base import Job, JobFuture
from .manager import Jobs
from .log import logger

if T.TYPE_CHECKING:
    from dask.distributed import Client


@dataclass
class EngineSetting:
    max_threads: T.Optional[int] = None
    max_processes: T.Optional[int] = None
    max_dask_jobs: T.Optional[int] = None
    max_jobs: T.Optional[int] = 20
    cache_path: T.Optional[str] = None
    print_traceback: bool = True


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
            loop: T.Optional[asyncio.AbstractEventLoop] = None,
            ) -> None:
        super().__init__()
        if setting is None:
            setting = EngineSetting()
        self.setting = setting
        self.print_traceback = False
        self.setup_by_setting()
        if jobs is None:
            jobs = Jobs(self.cache_dir / "jobs")
        self.jobs: Jobs = jobs
        self._dask_client: T.Optional["Client"] = None
        self._loop = loop
        self._loop_thread: T.Optional[Thread] = None

    def __repr__(self) -> str:
        return f"<Engine id={self.id}>"

    def __str__(self) -> str:
        return repr(self)

    @property
    def loop(self):
        if self._loop is None:
            loop = asyncio.new_event_loop()
            logger.info(f"{self} created new event loop.")
            self._loop = loop
        return self._loop

    @loop.setter
    def loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    def start(self):
        """Start event loop thread."""
        def run_in_thread(loop: asyncio.AbstractEventLoop):
            logger.info(f"{self} start event loop.")
            asyncio.set_event_loop(loop)
            loop.run_forever()

        if self._loop_thread is None:
            self._loop_thread = Thread(target=run_in_thread, args=(self.loop,))

        if not self._loop_thread.is_alive():
            logger.info(f"{self} start event loop thread.")
            self._loop_thread.start()
        else:
            logger.warning(f"Event loop thread of {self} is already running.")

    def stop(self):
        """Stop event loop thread."""
        loop = self._loop
        if (loop is None) or (self._loop_thread is None):
            logger.warning(f"Event loop thread of {self} is not running.")
            return

        if not self._loop_thread.is_alive():
            logger.warning(f"Event loop thread of {self} is already closed.")
        else:
            logger.info(f"{self} stop event loop.")
            loop.call_soon_threadsafe(loop.stop)
            self._loop_thread.join()
            if self._dask_client is not None:
                asyncio.run_coroutine_threadsafe(
                    self._dask_client.close(), loop)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    def setup_by_setting(self):
        setting = self.setting
        logger.info(f"Load setting: {setting}")
        self.resource = Resource(
            n_thread=setting.max_threads or float('inf'),
            n_process=setting.max_processes or float('inf'),
            n_dask=setting.max_dask_jobs or float('inf'),
            n_job=setting.max_jobs or float('inf'),
        )
        self.cache_dir = self.get_cache_dir()
        self.print_traceback = setting.print_traceback

    def submit(self, job: Job) -> JobFuture:
        """Submit job to engine and return a future object."""
        fut = asyncio.run_coroutine_threadsafe(
            self.submit_async(job), self.loop)
        fut.result()
        return job.future

    async def submit_async(self, job: Job):
        """Asynchronous interface for submit job to engine."""
        if job.status == "created":
            job.engine = self
            job._status = "pending"
            self.jobs.add(job)
        else:
            job.status = "pending"
        await job.emit()

    def remove(self, job: Job):
        """Remove job from engine."""
        if job.status in ('running', 'pending'):
            fut = asyncio.run_coroutine_threadsafe(
                job.cancel(), self.loop)
            fut.result()
        logger.info(f"Remove job from engine: {job}")
        self.jobs.remove(job)

    def cancel(self, job: Job):
        """Cancel a job."""
        fut = asyncio.run_coroutine_threadsafe(
            job.cancel(), self.loop)
        fut.result()

    def cancel_all(self):
        """Cancel all pending and running jobs."""
        running = self.jobs.running.values()
        pending = self.jobs.pending.values()
        futures = []
        for job in (pending + running):
            fut = asyncio.run_coroutine_threadsafe(
                job.cancel(), self.loop)
            futures.append(fut)
        concurrent.futures.wait(futures)

    def wait_job(
            self, job: T.Union[Job, JobFuture],
            timeout: T.Optional[float] = None,
            ) -> T.Optional[T.Any]:
        """Block until job is finished or timeout.
        Return job result if job is done."""
        if isinstance(job, JobFuture):
            job_ = self.jobs.get_job_by_id(job.job_id)
        else:
            job_ = job
        fut = asyncio.run_coroutine_threadsafe(
            job_.join(timeout=timeout), self.loop)
        fut.result()
        if job_.status == "done":
            return job_.result()
        else:
            return None

    def wait(
            self,
            timeout: T.Optional[float] = None,
            time_delta: float = 0.2):
        """Block until all jobs are finished or timeout."""
        total_time = timeout if timeout is not None else float('inf')
        while True:
            n_wait_jobs = len(self.jobs.running) + len(self.jobs.pending)
            if n_wait_jobs == 0:
                break
            if total_time <= 0:
                break
            time.sleep(time_delta)
            total_time -= time_delta

    async def join(self, timeout: T.Optional[float] = None):
        """Asynchronous interface for wait all jobs."""
        running = self.jobs.running.values()
        pending = self.jobs.pending.values()
        tasks = [
            asyncio.create_task(job.join())
            for job in (running + pending)
        ]
        await asyncio.wait(tasks, timeout=timeout)

    def get_cache_dir(self) -> Path:
        """Get cache directory for engine."""
        cache_path = self.setting.cache_path
        if cache_path is not None:
            path = cache_path
        else:
            path = f".executor/{self.id}"
        path_obj = Path(path)
        path_obj.mkdir(parents=True, exist_ok=True)
        return path_obj

    @property
    def dask_client(self):
        from .job.dask import get_default_client
        if self._dask_client is None:
            self._dask_client = get_default_client()
        return self._dask_client

    @dask_client.setter
    def dask_client(self, client: "Client"):
        if not client.asynchronous:
            raise ValueError("Dask client must be asynchronous.")
        self._dask_client = client
