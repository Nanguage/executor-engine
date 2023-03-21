import asyncio
import time

import pytest

from executor.engine.core import Engine
from executor.engine.job.dask import DaskJob
from dask.distributed import Client


def test_submit_job():
    job = DaskJob(lambda x: x**2, (2,))
    assert job.has_resource() is False
    assert job.consume_resource() is False
    assert job.release_resource() is False
    job2 = DaskJob(lambda x: x**2, (3,))

    with Engine() as engine:
        engine.submit(job)
        engine.submit(job2)
        engine.wait()
        assert job.result() == 4
        assert job2.result() == 9


def test_exception():
    engine = Engine()

    def error_func():
        raise ValueError("error")
    job = DaskJob(error_func)

    async def main():
        await engine.submit_async(job)
        await engine.join()
        assert job.status == "failed"
        await engine.dask_client.close()

    asyncio.run(main())


def test_cancel_job():
    engine = Engine()

    def sleep_func():
        time.sleep(10)
    job = DaskJob(sleep_func)

    async def main():
        await engine.submit_async(job)
        await asyncio.sleep(1)
        await job.cancel()
        assert job.status == "canceled"
        await engine.dask_client.close()

    asyncio.run(main())


def test_set_client():
    engine = Engine()

    async def main():
        client = Client(asynchronous=True)
        engine.dask_client = client
        assert engine.dask_client is client
        await asyncio.sleep(0.1)
        await client.close()
        client = Client()
        with pytest.raises(ValueError):
            engine.dask_client = client
        client.close()

    asyncio.run(main())
