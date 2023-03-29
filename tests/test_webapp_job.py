import asyncio
from http.server import HTTPServer, SimpleHTTPRequestHandler

import pytest

from executor.engine.core import Engine, EngineSetting
from executor.engine.job.extend.webapp import WebappJob
from executor.engine.job.condition import AfterAnother


def run_simple_httpd(ip: str, port: int):
    server_addr = (ip, port)
    httpd = HTTPServer(server_addr, SimpleHTTPRequestHandler)
    httpd.serve_forever()


setting = EngineSetting(print_traceback=False)


def test_run_webapp():
    engine = Engine()

    async def submit_job():
        with pytest.raises(NotImplementedError):
            job = WebappJob(run_simple_httpd, ip="2.2.2.2", port=8001, check_delta=0.5)

        with pytest.raises(ValueError):
            job = WebappJob("python -m http.server -b ip port")

        with pytest.raises(TypeError):
            job = WebappJob(1)

        job = WebappJob(run_simple_httpd, ip="127.0.0.1", port=8001, check_delta=0.5)
        await engine.submit_async(job)
        await asyncio.sleep(5)
        assert job.port == 8001
        assert job.status == "running"
        await job.cancel()
        assert job.status == "cancelled"

    asyncio.run(submit_job())


def test_port_check():
    engine = Engine(setting=setting)

    def run_error_httpd(ip: str, port: int):
        server_addr = (ip, port + 1)
        httpd = HTTPServer(server_addr, SimpleHTTPRequestHandler)
        httpd.serve_forever()

    async def submit_job():
        job = WebappJob(run_error_httpd, ip="127.0.0.1", check_delta=0.5)
        await engine.submit_async(job)
        await asyncio.sleep(5)
        assert job.status == "failed"

    asyncio.run(submit_job())


def test_launch_from_cmd():
    engine = Engine()

    async def submit_job():
        job = WebappJob("python -m http.server -b {ip} {port}")
        await engine.submit_async(job)
        await asyncio.sleep(5)
        assert job.status == "running"
        await job.cancel()
        assert job.status == "cancelled"
    
    asyncio.run(submit_job())


def test_launch_from_cmd_port_check():
    engine = Engine(setting=setting)

    async def submit_job():
        job = WebappJob("python -m http.server -b {ip} {port}1")
        await engine.submit_async(job)
        await asyncio.sleep(10)
        assert job.status == "failed"

    asyncio.run(submit_job())


def test_repr():
    job = WebappJob("python -m http.server -b {ip} {port}")
    repr(job)
    job1 = WebappJob(
        "python -m http.server -b {ip} {port}",
        condition=AfterAnother(job_id=job.id),
    )
    assert repr(job1.condition) in repr(job1)
