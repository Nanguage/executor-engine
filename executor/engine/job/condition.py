import typing as T
from datetime import datetime
from dataclasses import dataclass

from .utils import JobStatusType

if T.TYPE_CHECKING:
    from ..core import Engine


@dataclass
class Condition():
    """Base class for condition"""

    def satisfy(self, engine: "Engine") -> bool:  # pragma: no cover
        """Check if the condition is satisfied."""
        return True


@dataclass
class AfterAnother(Condition):
    """Condition that the job is executed after
    another job is done/failed/cancelled."""

    job_id: str
    statuses: T.Iterable[JobStatusType] = ("done", "failed", "cancelled")

    def satisfy(self, engine):
        """Check if the condition is satisfied."""
        try:
            another = engine.jobs.get_job_by_id(self.job_id)
        except Exception:
            return False
        if another.status in self.statuses:
            return True
        else:
            return False


@dataclass
class AfterOthers(Condition):
    """Condition that the job is executed after
    other jobs are done/failed/cancelled."""

    job_ids: T.List[str]
    statuses: T.Iterable[JobStatusType] = ("done", "failed", "cancelled")
    mode: T.Literal['all', 'any'] = "all"

    def satisfy(self, engine):
        """Check if the condition is satisfied."""
        other_job_satisfy = []
        for id_ in self.job_ids:
            try:
                job = engine.jobs.get_job_by_id(id_)
            except Exception:
                other_job_satisfy.append(False)
                continue
            s_ = job.status in self.statuses
            other_job_satisfy.append(s_)
        if self.mode == 'all':
            return all(other_job_satisfy)
        else:
            return any(other_job_satisfy)


@dataclass
class AfterTimepoint(Condition):
    """Condition that the job is executed after a timepoint."""

    timepoint: datetime

    def satisfy(self, engine):
        """Check if the condition is satisfied."""
        if datetime.now() > self.timepoint:
            return True
        else:
            return False


@dataclass
class Combination(Condition):
    """Base class for combination of conditions."""
    conditions: T.List[Condition]


@dataclass
class AllSatisfied(Combination):
    """Condition that the job is executed after all
    sub-conditions are satisfied."""

    def satisfy(self, engine):
        """Check if the condition is satisfied."""
        return all([c.satisfy(engine) for c in self.conditions])


@dataclass
class AnySatisfied(Combination):
    """Condition that the job is executed after any
    sub-condition is satisfied."""

    def satisfy(self, engine):
        """Check if the condition is satisfied."""
        return any([c.satisfy(engine) for c in self.conditions])
