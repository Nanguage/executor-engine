import inspect
import typing as T
from datetime import datetime
from copy import copy


if T.TYPE_CHECKING:
    from .base import Job


class Condition(object):
    def __init__(self):
        self.job: T.Optional["Job"] = None

    def set_job(self, job: T.Optional["Job"]):
        self.job = job

    def copy(self) -> "Condition":
        return copy(self)

    def satisfy(self) -> bool:
        return True

    def get_attrs_for_init(self) -> T.List[str]:
        init_mth = getattr(self, '__init__')
        sig = inspect.signature(init_mth)
        attr_names = [n for n in sig.parameters.keys()]
        return attr_names

    def __str__(self):
        cls_name = self.__class__.__name__
        attr_strs = " ".join([
            f"{a}={getattr(self, a)}"
            for a in self.get_attrs_for_init()
        ])
        s = f"<{cls_name} {attr_strs}>"
        return s

    def __repr__(self):
        return str(self)


class AfterAnother(Condition):
    def __init__(
            self,
            job_id: str,
            status: str = "done"):
        super().__init__()
        self.job_id = job_id
        self.status = status

    def satisfy(self) -> bool:
        job = self.job
        assert job is not None
        engine = job.engine
        assert engine is not None
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
    def __init__(self,
            job_ids: T.List[str],
            mode: T.Literal['all', 'any'] = 'all',
            status: str = "done"):
        super().__init__()
        assert mode in ('all', 'any')
        self.job_ids = job_ids
        self.mode = mode
        self.status = status

    def satisfy(self) -> bool:
        job = self.job
        assert job is not None
        engine = job.engine
        assert engine is not None
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
    def __init__(self, timepoint: datetime):
        super().__init__()
        assert isinstance(timepoint, datetime)
        self.timepoint = timepoint

    def satisfy(self) -> bool:
        if datetime.now() > self.timepoint:
            return True
        else:
            return False


class Combination(Condition):
    def __init__(self, conditions: T.List[Condition]):
        super().__init__()
        self.conditions = conditions

    def set_job(self, job: T.Optional["Job"]):
        for c in self.conditions:
            c.set_job(job)

    def copy(self) -> "Combination":
        cpy = copy(self)
        cpy.conditions = [copy(c) for c in cpy.conditions]
        return cpy


class AllSatisfied(Combination):
    def satisfy(self) -> bool:
        return all([c.satisfy() for c in self.conditions])


class AnySatisfied(Combination):
    def satisfy(self) -> bool:
        return any([c.satisfy() for c in self.conditions])
