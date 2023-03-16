import typing as T
from datetime import datetime

from pydantic import BaseModel

from .utils import JobStatusType

if T.TYPE_CHECKING:
    from ..core import Engine


class Condition(BaseModel):
    def satisfy(self, engine: "Engine") -> bool:  # pragma: no cover
        return True


class AfterAnother(Condition):
    job_id: str
    status: JobStatusType = "done"

    def satisfy(self, engine):
        another = engine.jobs.get_job_by_id(
            self.job_id)
        if another is None:
            return False
        else:
            if another.status == self.status:
                return True
            else:
                return False


class AfterOthers(Condition):
    job_ids: T.List[str]
    status: JobStatusType = "done"
    mode: T.Literal['all', 'any'] = "all"

    def satisfy(self, engine):
        other_job_satisfy = []
        for id_ in self.job_ids:
            job = engine.jobs.get_job_by_id(id_)
            if job is None:
                return False
            s_ = job.status == self.status
            other_job_satisfy.append(s_)
        if self.mode == 'all':
            return all(other_job_satisfy)
        else:
            return any(other_job_satisfy)


class AfterTimepoint(Condition):
    timepoint: datetime

    def satisfy(self, engine):
        if datetime.now() > self.timepoint:
            return True
        else:
            return False


class Combination(Condition):
    conditions: T.List[Condition]


class AllSatisfied(Combination):
    def satisfy(self, engine):
        return all([c.satisfy(engine) for c in self.conditions])


class AnySatisfied(Combination):
    def satisfy(self, engine):
        return any([c.satisfy(engine) for c in self.conditions])
