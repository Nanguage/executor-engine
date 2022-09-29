import asyncio

from executor.engine import Engine
from executor.job import LocalJob, ThreadJob, ProcessJob


def test_submit_job():
    engine = Engine()

    n_run = 0
    def callback(res):
        nonlocal n_run
        n_run += 1
        assert res == 4

    test_job_cls = [LocalJob, ThreadJob, ProcessJob]

    async def submit_job():
        for job_cls in test_job_cls:
            job = job_cls(lambda x: x**2, (2,), callback=callback)
            await engine.submit(job)
        await engine.wait()

    asyncio.run(submit_job())
    assert n_run == 3


def test_get_job_result():
    engine = Engine()

    test_job_cls = [LocalJob, ThreadJob, ProcessJob]

    async def submit_job():
        for job_cls in test_job_cls:
            job = job_cls(lambda x: x**2, (2,))
            await engine.submit(job)
            await job.task
            assert job.result() == 4

    asyncio.run(submit_job())
