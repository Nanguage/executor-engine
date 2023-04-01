import sys
import typing as T
from functools import update_wrapper
from pathlib import Path
import traceback


class Tee(object):
    def __init__(
            self, out_file: T.TextIO,
            stream: T.Literal['stdout', 'stderr'] = 'stdout'):
        self.stream_type = stream
        self.file = out_file
        self.stream: T.TextIO = getattr(sys, stream)

    def write(self, data):
        self.stream.write(data)
        self.file.write(data)

    def flush(self):
        self.stream.flush()
        self.file.flush()

    def __enter__(self):
        setattr(sys, self.stream_type, self)
        return self

    def __exit__(self, _type, _value, _traceback):
        setattr(sys, self.stream_type, self.stream)


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
        with open(self.stdout_file, 'w') as fo, \
             open(self.stderr_file, 'w') as fe:
            with Tee(fo, 'stdout'), Tee(fe, 'stderr'):
                try:
                    res = self.func(*args, **kwargs)
                except Exception as e:
                    if self.capture_traceback:
                        traceback.print_exc(file=fe)
                    raise e
        return res
