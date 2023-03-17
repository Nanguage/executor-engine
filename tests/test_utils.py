import subprocess as subp
import time

from executor.engine.utils import (
    CheckAttrRange,
    CheckAttrType,
    PortManager,
)
from executor.engine.error import (
    RangeCheckError, TypeCheckError
)

import pytest


def test_check_attr_range():
    class A:
        status = CheckAttrRange()
        status.attr = "_status"
        status.valid_range = ["created", "submit", "stoped"]

    a = A()
    a.status = "created"
    assert a._status == "created"
    with pytest.raises(RangeCheckError):
        a.status = "invalid_status"


def test_check_attr_type():
    class A:
        status = CheckAttrType()
        status.attr = "_status"
        status.valid_type = [
            int, str, lambda x: isinstance(x, list)]

    a = A()
    a.status = 1
    assert a.status == 1
    a.status = "1"
    assert a.status == "1"
    a.status = ["1"]
    assert a.status == ["1"]

    with pytest.raises(TypeCheckError):
        a.status = {}


def test_port_manager():
    manager = PortManager()
    ip = "127.0.0.1"
    port = manager.get_port()
    p = subp.Popen(
        ["python", "-m", "http.server", "-b", f"{ip}", f"{port}",],
    )
    # wait for process start
    time.sleep(2)
    assert manager.process_has_port(p.pid, ip, port)
    p.kill()
