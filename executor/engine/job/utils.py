import typing as T
from datetime import datetime

from ..utils import CheckAttrRange
from ..error import ExecutorError


if T.TYPE_CHECKING:
    from .base import Job


JobStatusType = T.Literal['pending', 'running', 'failed', 'done', 'canceled']
valid_job_statuses = JobStatusType.__args__  # type: ignore


class JobStatusAttr(CheckAttrRange):
    valid_range: T.Iterable[JobStatusType] = valid_job_statuses
    attr = "_status"

    def __set__(self, obj: "Job", value: JobStatusType):
        self.check(obj, value)
        if obj.engine is not None:
            obj.engine.jobs.move_job_store(obj, value)
        setattr(obj, self.attr, value)
        if value in ('done', 'failed', 'canceled'):
            obj.stoped_time = datetime.now()


class JobEmitError(ExecutorError):
    pass


class InvalidStateError(ExecutorError):
    pass
