import asyncio
import time

import pytest

from executor.engine.core import Engine, EngineSetting
from executor.engine.job.dask import DaskJob
from executor.engine.utils import PortManager
from dask.distributed import Client, LocalCluster


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
    engine = Engine(
        setting=EngineSetting(print_traceback=False)
    )

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
        assert job.status == "cancelled"
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
        port = PortManager.find_free_port()
        cluster = LocalCluster(dashboard_address=f":{port}")
        client = Client(cluster)
        with pytest.raises(ValueError):
            engine.dask_client = client
        client.close()

    asyncio.run(main())


@pytest.mark.asyncio
async def test_dask_generator():
    port = PortManager.find_free_port()
    cluster = LocalCluster(
        dashboard_address=f":{port}",
        asynchronous=True,
        processes=False,
    )
    client = Client(cluster)
    engine = Engine()
    engine.dask_client = client

    async def gen():
        for i in range(10):
            yield i

    job = DaskJob(gen)
    await engine.submit_async(job)
    await job.wait_until_status("running")
    assert job.status == "running"
    g = job.result()
    i = 0
    async for x in g:
        assert x == i
        i += 1
    assert job.status == "done"


@pytest.mark.asyncio
async def test_dask_async_func():
    port = PortManager.find_free_port()
    cluster = LocalCluster(
        dashboard_address=f":{port}",
        asynchronous=True,
        processes=False,
    )
    client = Client(cluster)
    engine = Engine()
    engine.dask_client = client

    async def async_func(x):
        return x + 1

    job = DaskJob(async_func, (1,))
    await engine.submit_async(job)
    await job.wait_until_status("done")
    assert job.result() == 2
