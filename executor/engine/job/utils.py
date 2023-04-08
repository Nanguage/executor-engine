import typing as T
from datetime import datetime

from ..utils import CheckAttrRange, ExecutorError


if T.TYPE_CHECKING:
    from .base import Job


JobStatusType = T.Literal['pending', 'running', 'failed', 'done', 'cancelled']
valid_job_statuses: T.List[JobStatusType] = [
    'pending', 'running', 'failed', 'done', 'cancelled']


class JobStatusAttr(CheckAttrRange):
    valid_range: T.Iterable[JobStatusType] = valid_job_statuses
    attr = "_status"

    def __set__(self, obj: "Job", value: JobStatusType):
        self.check(obj, value)
        old_status = getattr(obj, self.attr)
        setattr(obj, self.attr, value)
        if obj.engine is not None:
            obj.engine.jobs.move_job_store(
                obj, value, old_status)
        if value in ('done', 'failed', 'cancelled'):
            obj.stoped_time = datetime.now()


class InvalidStateError(ExecutorError):
    def __init__(self, job: "Job", valid_status: T.List[JobStatusType]):
        self.valid_status = valid_status
        super().__init__(
            f"Invalid state: {job} is in {job.status} state, "
            f"but should be in {valid_status} state.")
