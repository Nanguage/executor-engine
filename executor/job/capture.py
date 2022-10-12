import typing as T
from functools import update_wrapper
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path


class CaptureOut(object):
    def __init__(
            self, func: T.Callable,
            stdout_file: Path, stderr_file: Path):
        update_wrapper(func, self)
        self.func = func
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file

    def __call__(self, *args, **kwargs) -> T.Any:
        with open(self.stdout_file, 'w') as fo, open(self.stderr_file, 'w') as fe:
            with redirect_stdout(fo), redirect_stderr(fe):
                res = self.func(*args, **kwargs)
        return res
