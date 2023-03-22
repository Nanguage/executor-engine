import typing as T
from datetime import datetime
from dataclasses import dataclass

from .utils import JobStatusType

if T.TYPE_CHECKING:
    from ..core import Engine


@dataclass
class Condition():
    def satisfy(self, engine: "Engine") -> bool:  # pragma: no cover
        return True


@dataclass
class AfterAnother(Condition):
    job_id: str
    statuses: T.Iterable[JobStatusType] = ("done", "failed", "cancelled")

    def satisfy(self, engine):
        another = engine.jobs.get_job_by_id(
            self.job_id)
        if another.status in self.statuses:
            return True
        else:
            return False


@dataclass
class AfterOthers(Condition):
    job_ids: T.List[str]
    statuses: T.Iterable[JobStatusType] = ("done", "failed", "cancelled")
    mode: T.Literal['all', 'any'] = "all"

    def satisfy(self, engine):
        other_job_satisfy = []
        for id_ in self.job_ids:
            job = engine.jobs.get_job_by_id(id_)
            s_ = job.status in self.statuses
            other_job_satisfy.append(s_)
        if self.mode == 'all':
            return all(other_job_satisfy)
        else:
            return any(other_job_satisfy)


@dataclass
class AfterTimepoint(Condition):
    timepoint: datetime

    def satisfy(self, engine):
        if datetime.now() > self.timepoint:
            return True
        else:
            return False


@dataclass
class Combination(Condition):
    conditions: T.List[Condition]


@dataclass
class AllSatisfied(Combination):
    def satisfy(self, engine):
        return all([c.satisfy(engine) for c in self.conditions])


@dataclass
class AnySatisfied(Combination):
    def satisfy(self, engine):
        return any([c.satisfy(engine) for c in self.conditions])
