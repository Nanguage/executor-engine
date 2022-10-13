import typing as T
from functools import update_wrapper
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
import traceback


class CaptureOut(object):
    def __init__(
            self, func: T.Callable,
            stdout_file: Path, stderr_file: Path,
            capture_traceback: bool = True):
        update_wrapper(func, self)
        self.func = func
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file
        self.capture_traceback = capture_traceback

    def __call__(self, *args, **kwargs) -> T.Any:
        with open(self.stdout_file, 'w') as fo, open(self.stderr_file, 'w') as fe:
            with redirect_stdout(fo), redirect_stderr(fe):
                try:
                    res = self.func(*args, **kwargs)
                except Exception as e:
                    if self.capture_traceback:
                        traceback.print_exc(file=fe)
                    raise e
        return res
