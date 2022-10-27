import typing as T
from dataclasses import dataclass
from pathlib import Path

from .base import ExecutorObj
from .job.base import Job
from .jobs import Jobs


@dataclass
class EngineSetting:
    max_threads: int = 20
    max_processes: int = 8
    cache_path: T.Optional[str] = None


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

    def setup_by_setting(self):
        setting = self.setting
        self.thread_count = setting.max_threads
        self.process_count = setting.max_processes
        self.cache_dir = self.get_cache_dir()

    async def submit(self, job: Job):
        if job.status == "created":
            job.engine = self
            job._status = "pending"
            self.jobs.add(job)
        else:
            job.status = "pending"
        await job.emit()

    async def wait(self):
        job: Job
        for job in self.jobs.all_jobs():
            if job.task is not None:
                await job.task

    def get_cache_dir(self) -> Path:
        cache_path = self.setting.cache_path
        if cache_path is not None:
            path =  cache_path
        else:
            path = f".executor/{self.id}"
        path_obj = Path(path)
        path_obj.mkdir(parents=True, exist_ok=True)
        return path_obj

    def cancel_all(self):
        for job in self.jobs.running.values():
            job.status = 'canceled'
