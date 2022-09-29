import typing as T

from .base import Job
from .utils import ThreadWithExc
from ..error import ExecutorError


class StopIThread(ExecutorError):
    pass


class IThread(ThreadWithExc):
    def __init__(
            self,
            func: T.Callable, args: tuple, kwargs: dict,
            callback: T.Callable[[T.Any], None],
            error_callback: T.Callable[[Exception], None],
            ) -> None:
        super().__init__()
        self.func = func
        self._args = args
        self._kwargs = kwargs
        self.callback = callback
        self.error_callback = error_callback

    def run(self):
        success = False
        try:
            res = self.func(*self._args, **self._kwargs)
            success = True
        except StopIteration:
            pass
        except Exception as e:
            self.error_callback(e)
        if success:
            self.callback(res)


class ThreadJob(Job):
    def has_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            return self.engine.thread_count > 0

    def consume_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.thread_count -= 1
            return True

    def release_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.thread_count += 1
            return True

    def run(self):
        self._thread = IThread(
            func=self.func,
            args=self.args, kwargs=self.kwargs,
            callback=self.on_done,
            error_callback=self.on_failed)
        self._thread.start()

    def cancel_task(self):
        if self._thread.is_alive():
            try:
                self._thread.raiseExc(StopIteration)
            except Exception:
                pass
        del self._thread
