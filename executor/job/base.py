import typing as T
from queue import Queue
from datetime import datetime

from ..utils import CheckAttrRange
from ..error import ExecutorError
from ..base import ExecutorObj


if T.TYPE_CHECKING:
    from ..engine import Engine


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


class JobEmitError(ExecutorError):
    pass


class Job(ExecutorObj):

    status = JobStatusAttr()

    def __init__(
            self,
            func: T.Callable, args: tuple,
            callback: T.Callable[[T.Any], None],
            error_callback: T.Callable[[Exception], None],
            name: T.Optional[str] = None,
            **kwargs
            ) -> None:
        super().__init__()
        self.func = func
        self.args = args
        self.callback = callback
        self.error_callback = error_callback
        self.engine: T.Optional["Engine"] = None
        self._status: str = "pending"
        self._for_join: Queue = Queue()
        self.name = name or func.__name__
        self.attrs = kwargs

    def __repr__(self) -> str:
        return f"<Job status={self.status} id={self.id[-8:]} func={self.func}>"

    def has_resource(self) -> bool:
        return True

    def consume_resource(self) -> bool:
        return True

    def release_resource(self) -> bool:
        return True

    def emit(self):
        _valid_status = ("pending", "canceled", "done", "failed")
        if self.status not in _valid_status:
            raise JobEmitError(
                f"{self} is not in valid status({_valid_status})")
        self.status = "running"
        self._for_join.put(0)
        self.run()

    def _on_finish(self, new_status: JobStatusType = "done"):
        self.status = new_status
        self.release_resource()
        if self.engine is not None:
            self.engine.activate()
        self._for_join.task_done()
        self._for_join.get()

    def on_done(self, res):
        if self.callback is not None:
            self.callback(res)
        self._on_finish("done")

    def on_failed(self, e: Exception):
        if self.error_callback is not None:
            self.error_callback(e)
        self._on_finish("failed")

    def join(self):
        self._for_join.join()

    def run(self):
        pass

    def cancel(self):
        if self.status == "running":
            try:
                self.cancel_task()
            except Exception as e:
                print(str(e))
            finally:
                self._on_finish("canceled")
        elif self.status == "pending":
            self.engine.jobs.pending.pop(self.id)

    def cancel_task(self):
        pass

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'status': self.status,
            'check_time': str(datetime.now()),
        }


class LocalJob(Job):
    def run(self):
        success = False
        try:
            res = self.func(*self.args)
            success = True
        except Exception as e:
            self.on_failed(e)
        if success:
            self.on_done(res)
