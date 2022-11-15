import asyncio
from http.server import HTTPServer, SimpleHTTPRequestHandler

from executor.engine.core import Engine
from executor.engine.job.extend.webapp import WebAppJob


def run_simple_httpd(ip: str, port: int):
    server_addr = (ip, port)
    httpd = HTTPServer(server_addr, SimpleHTTPRequestHandler)
    httpd.serve_forever()


def test_run_webapp():
    engine = Engine()

    async def submit_job():
        job = WebAppJob(run_simple_httpd, ip="127.0.0.1", port=8001)
        await engine.submit(job)
        await asyncio.sleep(0.5)
        assert job.status == "running"
        await job.cancel()
        assert job.status == "canceled"

    asyncio.run(submit_job())
