import typing as T
import asyncio
import contextlib

from .error import RangeCheckError, TypeCheckError


class CheckAttrRange(object):
    valid_range: T.Iterable[T.Any] = []
    attr = "__"

    def __get__(self, obj, objtype=None):
        return getattr(obj, self.attr)

    def check(self, obj, value):
        if value not in self.valid_range:
            raise RangeCheckError(
                f"{obj}'s {type(self)} attr should be "
                f"one of: {self.valid_range}"
            )

    def __set__(self, obj, value):
        self.check(obj, value)
        setattr(obj, self.attr, value)


Checker = T.Callable[[T.Any], bool]


class CheckAttrType(object):
    valid_type: T.List[T.Union[Checker, type]] = []
    attr = "__"

    def __get__(self, obj, objtype=None):
        return getattr(obj, self.attr)

    def __set__(self, obj, value):
        check_passed = []
        for tp in self.valid_type:
            if isinstance(tp, type):
                passed = isinstance(value, tp)
            else:
                assert isinstance(tp, T.Callable)
                passed = tp(value)
            check_passed.append(passed)
        is_valid_type = any(check_passed)
        if not is_valid_type:
            raise TypeCheckError(
                f"{obj}'s {type(self)} attr should in type: "
                f"{self.valid_type}"
            )
        setattr(obj, self.attr, value)


@contextlib.contextmanager
def event_loop():
    loop, is_new_loop = get_event_loop()
    try:
        yield loop
    finally:
        if is_new_loop:
            loop.close()


def get_event_loop() -> T.Tuple[asyncio.AbstractEventLoop, bool]:
    is_new_loop = False
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        is_new_loop = True
    return loop, is_new_loop