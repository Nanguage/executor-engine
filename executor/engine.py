from collections import OrderedDict
import typing as T
from dataclasses import dataclass
from pathlib import Path

from .base import ExecutorObj
from .job.base import Job
from .job.utils import valid_job_statuses, JobStatusType


@dataclass
class EngineSetting:
    max_threads: int = 20
    max_processes: int = 8
    cache_path: T.Optional[str] = None


JobStoreType = T.OrderedDict[str, Job]


class Jobs:
    valid_statuses = valid_job_statuses

    def __init__(self):
        self._stores: T.Dict[str, JobStoreType] = {
            s: OrderedDict() for s in self.valid_statuses
        }
        self.pending = self._stores['pending']
        self.running = self._stores['running']
        self.done = self._stores['done']
        self.failed = self._stores['failed']
        self.canceled = self._stores['canceled']

    def clear(self, statuses: T.List[JobStatusType]):
        for s in statuses:
            self._stores[s].clear()

    def clear_non_active(self):
        self.clear(["done", "failed", "cannceled"])

    def clear_all(self):
        self.clear(self.valid_statuses)

    def add(self, job: Job):
        store = self._stores[job.status]
        store[job.id] = job

    def remove(self, job: Job):
        for tp in self.valid_statuses:
            store = self._stores[tp]
            if job.id in store:
                store.pop(job.id)

    def move_job_store(self, job: "Job", new_status: JobStatusType):
        if job.status == new_status:
            return
        old_store = self._stores[job.status]
        new_store = self._stores[new_status]
        new_store[job.id] = old_store.pop(job.id)

    def get_job_by_id(self, job_id: str) -> T.Optional["Job"]:
        for status in self.valid_statuses:
            store = self._stores[status]
            if job_id in store:
                return store[job_id]
        return None

    def all_jobs(self):
        jobs = []
        for status in self.valid_statuses:
            store = self._stores[status]
            for job in store.values():
                jobs.append(job)
        return jobs


class Engine(ExecutorObj):
    def __init__(
            self,
            setting: T.Optional[EngineSetting] = None,
            jobs: T.Optional[Jobs] = None,
            ) -> None:
        super().__init__()
        if jobs is None:
            jobs = Jobs()
        self.jobs = jobs
        if setting is None:
            setting = EngineSetting()
        self.setting = setting
        self.setup_by_setting()

    def setup_by_setting(self):
        setting = self.setting
        self.thread_count = setting.max_threads
        self.process_count = setting.max_processes

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

    @property
    def cache_dir(self) -> Path:
        _cache_path = self.setting.cache_path
        if _cache_path is not None:
            path =  _cache_path
        else:
            path = f".executor/{self.id}"
        path_obj = Path(path)
        path_obj.mkdir(parents=True, exist_ok=True)
        return path_obj
