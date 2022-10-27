import typing as T
from pathlib import Path

from diskcache import Cache

from .job.base import Job
from .job.utils import valid_job_statuses, JobStatusType

if T.TYPE_CHECKING:
    from .core import Engine


class JobStore():
    """Store jobs"""
    def __init__(self, cache_path: T.Optional[Path]):
        if cache_path is not None:
            self.cache = Cache(str(cache_path))
        else:
            self.cache = None
        self.mem: T.Dict[str, Job] = dict()

    @classmethod
    def load_from_cache(cls, path: Path, engine: "Engine"):
        store = cls(path)
        job: Job
        for key in store.cache:
            bytes_ = store.cache[key]
            job = Job.deserialization(bytes_, engine)
            store.mem[key] = job
        return store

    def __setitem__(self, key: str, val: Job):
        self.mem[key] = val
        if self.cache is not None:
            bytes_ = val.serialization()
            self.cache[key] = bytes_

    def __getitem__(self, key: str) -> Job:
        return self.mem[key]

    def __contains__(self, key: str) -> bool:
        return key in self.mem

    def clear(self):
        self.mem.clear()
        self.cache.clear()

    def pop(self, key: str) -> Job:
        job = self.mem.pop(key)
        if self.cache is not None:
            self.cache.pop(key)
        return job

    def values(self) -> T.List[Job]:
        vals = list(self.mem.values())
        return vals

    def keys(self) -> T.List[str]:
        return list(self.mem.keys())

    def items(self) -> T.List[T.Tuple[str, Job]]:
        return list(self.mem.items())

    def __del__(self):
        if self.cache is not None:
            self.cache.close()


class Jobs:
    """Jobs manager."""
    valid_statuses = valid_job_statuses

    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self._stores: T.Dict[str, JobStore] = {}
        s: str
        for s in self.valid_statuses:
            path = cache_path / s
            store = JobStore(path)
            self._stores[s] = store
        self.set_attrs_for_read()

    def set_attrs_for_read(self):
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
