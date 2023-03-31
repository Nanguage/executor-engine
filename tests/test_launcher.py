import time
import asyncio
from http.server import HTTPServer, SimpleHTTPRequestHandler

import pytest

from executor.engine.launcher import (
    launcher, get_default_engine
)
from executor.engine import Engine
from cmd2func import cmd2func


def test_sync_launcher():
    @launcher(job_type='process')
    def add(a, b):
        time.sleep(1)
        return a + b

    assert add.async_mode is False

    with Engine() as engine:
        job = add.submit(1, 2)
        assert job.status == 'running'
        engine.wait_job(job)
        assert job.result() == 3


def test_sync_launcher_with_callback():
    @launcher
    def add(a, b):
        return a + b

    with Engine() as engine:
        var = 1

        def set_var(res):
            nonlocal var
            var = res

        job = add.submit(1, 2)
        job.future.add_done_callback(lambda x: set_var(x))
        engine.wait_job(job)
        assert var == 3


def test_sync_chain():
    @launcher
    def add(a, b):
        return a + b

    with Engine() as engine:
        job = add.submit(1, 2)
        job2 = add.submit(job.future, 2)
        engine.wait_job(job2)
        assert job2.result() == 5


def test_sync_launcher_call():
    @launcher
    def add(a, b):
        return a + b

    @launcher
    def raise_exception():
        raise ValueError("test")

    with Engine():
        assert add(1, 2) == 3
        with pytest.raises(ValueError):
            raise_exception()
        with pytest.raises(RuntimeError):
            job1 = raise_exception.submit()
            job2 = add.submit(job1.future, 2)
            add(job2.future, 2)


def test_async_launcher_run():
    @launcher(async_mode=True)
    def add(a, b):
        time.sleep(0.5)
        return a + b

    assert add.async_mode is True

    async def main():
        job = await add.submit(1, 2)
        await job.join()
        assert job.result() == 3
        a = await add(1, 2)
        assert a == 3
        b = await add(a, 2)
        assert b == 5

    asyncio.run(main())


def test_sync_async_convert():
    @launcher(async_mode=True)
    def add(a, b):
        time.sleep(0.5)
        return a + b

    assert add.async_mode is True
    add_sync = add.to_sync()
    assert add_sync.async_mode is False
    add_async = add_sync.to_async()
    assert add_async.async_mode is True


def test_cmd2func_launcher():
    @launcher
    @cmd2func
    def add(a, b):
        return f"python -c 'print({a} + {b})'"

    assert add.job_type == 'subprocess'

    @launcher
    @cmd2func
    def add2(a, b):
        yield f"python -c 'print({a} + {b})'"

    with Engine() as engine:
        job = add.submit(1, 2)
        job2 = add2.submit(1, 2)
        engine.wait()
        assert job.result() == 0
        assert job2.result() == 0


def test_webapp_launcer():
    @launcher(job_type="webapp")
    def simple_httpd(ip, port):
        server_addr = (ip, port)
        httpd = HTTPServer(server_addr, SimpleHTTPRequestHandler)
        httpd.serve_forever()

    with Engine() as engine:
        job = simple_httpd.submit('127.0.0.1', None)
        time.sleep(2)
        assert job.status == 'running'
        engine.cancel(job)


def test_cmd_webapp_launcher():
    @launcher(job_type='webapp')
    @cmd2func
    def simple_webapp():
        return "python -m http.server -b {ip} {port}"

    with Engine() as engine:
        job = simple_webapp.submit()
        time.sleep(2)
        assert job.status == 'running'
        engine.cancel(job)


def test_set_get_engine():
    @launcher
    def add(a, b):
        return a + b

    assert add.engine is get_default_engine()
    engine = Engine()
    add.engine = engine
    assert add.engine is engine
