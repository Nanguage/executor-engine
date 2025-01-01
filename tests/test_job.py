import asyncio
import time

import pytest

from executor.engine.core import Engine, EngineSetting
from executor.engine.job import LocalJob, ThreadJob, ProcessJob
from executor.engine.job.base import InvalidStateError
from executor.engine.job.condition import AfterAnother, AllSatisfied


def test_corner_cases():
    job = LocalJob(lambda x: x**2, (2,))
    assert job.has_resource() is False
    assert job.consume_resource() is False
    assert job.release_resource() is False

    job = ThreadJob(lambda x: x**2, (2,))
    assert job.has_resource() is False
    assert job.consume_resource() is False
    assert job.release_resource() is False

    job = ProcessJob(lambda x: x**2, (2,))
    assert job.has_resource() is False
    assert job.consume_resource() is False
    assert job.release_resource() is False
    assert job.runnable() is False
    assert job.cache_dir is None

    async def submit_job():
        with pytest.raises(InvalidStateError):
            await job.emit()

    asyncio.run(submit_job())

    async def submit_job():
        with pytest.raises(InvalidStateError):
            await job.rerun()

    asyncio.run(submit_job())

    async def submit_job():
        with pytest.raises(InvalidStateError):
            await job.join()

    asyncio.run(submit_job())


def test_result_fetch_error():
    def sleep_2s():
        time.sleep(2)

    job = ProcessJob(sleep_2s)

    engine = Engine()

    async def submit_job():
        with pytest.raises(InvalidStateError):
            job.result()
        await engine.submit_async(job)
        with pytest.raises(InvalidStateError):
            job.result()

    asyncio.run(submit_job())


def test_job_retry():
    def raise_exception():
        print("try")
        raise ValueError("error")
    job = ProcessJob(
        raise_exception, retries=2,
        retry_time_delta=1)
    assert job.retry_remain == 2
    setting = EngineSetting(print_traceback=False)
    engine = Engine(setting=setting)
    with engine:
        engine.submit(job)
        time.sleep(5)
    assert job.retry_remain == 0


def test_dependency():
    def add(a, b):
        return a + b

    with Engine() as engine:
        job1 = ProcessJob(add, (1, 2))
        job2 = ProcessJob(add, (job1.future, 3))
        engine.submit(job1, job2)
        engine.wait_job(job2)
        assert job2.result() == 6


def test_dependency_2():
    def add(a, b):
        return a + b

    with Engine() as engine:
        job1 = ProcessJob(add, (1, 2))
        job2 = ProcessJob(add, (job1.future, 3))
        job3 = ProcessJob(
            add, kwargs={"a": job2.future, "b": 4},
            condition=AfterAnother(job_id=job1.id)
        )
        engine.submit(job3, job2, job1)
        assert isinstance(job3.condition, AllSatisfied)
        engine.wait_job(job3)
        assert job3.result() == 10


def test_upstream_failed():
    def add(a, b):
        return a + b

    def raise_exception():
        raise ValueError("error")

    with Engine() as engine:
        job1 = ProcessJob(raise_exception)
        job2 = ProcessJob(add, (1, job1.future))
        engine.submit(job2, job1)
        engine.wait()
        assert job1.status == "failed"
        assert job2.status == "cancelled"


def test_upstream_cancel():
    def add(a, b):
        time.sleep(3)
        return a + b

    with Engine() as engine:
        job1 = ProcessJob(add, (1, 2))
        job2 = ProcessJob(add, (1, job1.future))
        engine.submit(job2, job1)
        engine.cancel(job1)
        engine.wait()
        assert job1.status == "cancelled"
        assert job2.status == "cancelled"


@pytest.mark.asyncio
async def test_async_callback():
    def add(a, b):
        return a + b

    async def callback(res):
        assert res == 3

    with Engine() as engine:
        job1 = ProcessJob(add, (1, 2), callback=callback)
        await engine.submit_async(job1)
        await job1.join()


@pytest.mark.asyncio
async def test_async_err_callback():
    def raise_err():
        raise ValueError("test")

    async def err_callback(e):
        print(e)

    setting = EngineSetting(print_traceback=False)
    with Engine(setting) as engine:
        job1 = ProcessJob(raise_err, error_callback=err_callback)
        await engine.submit_async(job1)
        await job1.join()


@pytest.mark.asyncio
async def test_wait_until():
    def add(a, b):
        time.sleep(3)
        return a + b

    with Engine() as engine:
        job1 = ProcessJob(add, (1, 2))
        job2 = ProcessJob(add, (1, 2))
        engine.submit(job1, job2)
        await job1.wait_until_status("running")
        assert job1.status == "running"
        with pytest.raises(asyncio.TimeoutError):
            await job2.wait_until_status("done", timeout=1)
        engine.wait()


@pytest.mark.asyncio
async def test_generator():
    with Engine() as engine:
        def gen():
            for i in range(10):
                yield i

        job = ProcessJob(gen)
        await engine.submit_async(job)
        await job.wait_until_status("running")
        assert job.status == "running"
        g = job.result()
        assert list(g) == list(range(10))
        assert job.status == "done"

        job = ThreadJob(gen)
        await engine.submit_async(job)
        await job.wait_until_status("running")
        assert job.status == "running"
        g = job.result()
        assert list(g) == list(range(10))
        assert job.status == "done"

        job = LocalJob(gen)
        await engine.submit_async(job)
        await job.wait_until_status("running")
        assert job.status == "running"
        g = job.result()
        assert list(g) == list(range(10))
        assert job.status == "done"


@pytest.mark.asyncio
async def test_generator_async():
    with Engine() as engine:
        async def gen_async(n):
            for i in range(n):
                yield i

        job = ProcessJob(gen_async, (10,))
        await engine.submit_async(job)
        await job.wait_until_status("running")
        res = []
        async for i in job.result():
            assert job.status == "running"
            res.append(i)
        assert job.status == "done"
        assert res == list(range(10))

        job = ThreadJob(gen_async, (10,))
        await engine.submit_async(job)
        await job.wait_until_status("running")
        res = []
        async for i in job.result():
            assert job.status == "running"
            res.append(i)
        assert job.status == "done"
        assert res == list(range(10))

        job = LocalJob(gen_async, (10,))
        await engine.submit_async(job)
        await job.wait_until_status("running")
        res = []
        async for i in job.result():
            assert job.status == "running"
            res.append(i)
        assert job.status == "done"
        assert res == list(range(10))


@pytest.mark.asyncio
async def test_generator_error():
    with Engine() as engine:
        def gen_error():
            for i in range(2):
                print(i)
                yield i
            raise ValueError("error")

        job = ProcessJob(gen_error)
        await engine.submit_async(job)
        await job.wait_until_status("running")
        with pytest.raises(ValueError):
            for i in job.result():
                assert job.status == "running"
        assert job.status == "failed"

        async def gen_error():
            for i in range(2):
                print(i)
                yield i
            raise ValueError("error")

        job = ProcessJob(gen_error)
        await engine.submit_async(job)
        await job.wait_until_status("running")
        with pytest.raises(ValueError):
            async for i in job.result():
                assert job.status == "running"
        assert job.status == "failed"


@pytest.mark.asyncio
async def test_generator_send():
    with Engine() as engine:
        def gen():
            res = 0
            for _ in range(3):
                res += yield res

        job = ProcessJob(gen)
        await engine.submit_async(job)
        await job.wait_until_status("running")
        assert job.status == "running"
        g = job.result()
        assert g.send(None) == 0
        assert g.send(1) == 1
        assert g.send(2) == 3
        with pytest.raises(StopIteration):
            g.send(3)
        assert job.status == "done"

        async def gen_async():
            res = 0
            for _ in range(3):
                res += yield res

        job = ProcessJob(gen_async)
        await engine.submit_async(job)
        await job.wait_until_status("running")
        assert job.status == "running"
        g = job.result()
        assert await g.asend(None) == 0
        assert await g.asend(1) == 1
        assert await g.asend(2) == 3
        with pytest.raises(StopAsyncIteration):
            await g.asend(3)
        assert job.status == "done"


@pytest.mark.asyncio
async def test_generator_send_localjob():
    with Engine() as engine:
        def gen():
            res = 0
            for _ in range(3):
                res += yield res

        job = LocalJob(gen)
        engine.submit(job)
        await job.wait_until_status("running")
        g = job.result()
        assert g.send(None) == 0
        assert g.send(1) == 1
        assert g.send(2) == 3
        with pytest.raises(StopIteration):
            g.send(3)

        # test async generator
        async def gen_async():
            res = 0
            for _ in range(3):
                res += yield res

        job = LocalJob(gen_async)
        engine.submit(job)
        await job.wait_until_status("running")
        g = job.result()
        assert await g.asend(None) == 0
        assert await g.asend(1) == 1
        assert await g.asend(2) == 3
        with pytest.raises(StopAsyncIteration):
            await g.asend(3)


@pytest.mark.asyncio
async def test_async_func_job():
    with Engine() as engine:
        async def async_func(x):
            return x + 1

        for job_cls in [LocalJob, ThreadJob, ProcessJob]:
            job = job_cls(async_func, (1,))
            await engine.submit_async(job)
            await job.wait_until_status("done")
            assert job.result() == 2
