from collections import OrderedDict
import typing as T
from dataclasses import dataclass
import time

from .job.base import Job, valid_job_statuses, JobStatusType


@dataclass
class EngineSetting:
    max_threads: int = 20
    max_processes: int = 8


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


class Engine(object):
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

    def submit(self, job: Job):
        assert job.status == "pending"
        job.engine = self
        self.jobs.add(job)
        self.activate()

    def activate(self):
        for j in self.jobs.pending.values():
            if j.has_resource() and j.consume_resource():
                j.emit()
                break

    def wait(
            self, time_interval=0.01,
            print_running_jobs: bool = False):
        while True:
            if len(self.jobs.running) == 0:
                break
            if print_running_jobs:
                print(list(self.jobs.running.values()))
            time.sleep(time_interval)
