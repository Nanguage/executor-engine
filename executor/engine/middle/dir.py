import os
import typing as T
from pathlib import Path
from functools import update_wrapper


class ChDir(object):
    def __init__(self, func: T.Callable, target_dir: Path):
        update_wrapper(func, self)
        self.func = func
        self.target_dir = target_dir

    def __call__(self, *args, **kwargs) -> T.Any:
        os.chdir(self.target_dir)
        return self.func(*args, **kwargs)
