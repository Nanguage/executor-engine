import asyncio
import time

import pytest

from executor.engine.core import Engine
from executor.engine.job.dask import DaskJob
from dask.distributed import Client


def test_submit_job():
    engine = Engine()
    job = DaskJob(lambda x: x**2, (2,))
    assert job.has_resource() is False
    assert job.consume_resource() is False
    assert job.release_resource() is False

    async def main():
        await engine.submit(job)
        await engine.wait()
        assert job.result() == 4

    asyncio.run(main())


def test_exception():
    engine = Engine()

    def error_func():
        raise ValueError("error")
    job = DaskJob(error_func)

    async def main():
        await engine.submit(job)
        await engine.wait()
        assert job.status == "failed"

    asyncio.run(main())


def test_cancel_job():
    engine = Engine()

    def sleep_func():
        time.sleep(10)
    job = DaskJob(sleep_func)

    async def main():
        await engine.submit(job)
        await asyncio.sleep(2)
        await job.cancel()
        await engine.wait()
        assert job.status == "canceled"

    asyncio.run(main())


def test_set_client():
    engine = Engine()

    async def main():
        client = Client(asynchronous=True)
        engine.dask_client = client
        assert engine.dask_client is client
        client = Client()
        with pytest.raises(ValueError):
            engine.dask_client = client

    asyncio.run(main())
