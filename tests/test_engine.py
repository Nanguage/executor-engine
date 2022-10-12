import asyncio
from datetime import datetime, timedelta

from executor.engine import Engine
from executor.job import LocalJob, ThreadJob, ProcessJob, Job
from executor.job.condition import AfterAnother, AfterOthers, AfterTimepoint


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
            job: Job = job_cls(lambda x: x**2, (2,))
            await engine.submit(job)
            await job.join()
            assert job.result() == 4

    asyncio.run(submit_job())


def test_after_another_cond():
    engine = Engine()

    lis = []
    def append(x):
        lis.append(x)

    async def submit_job():
        job1 = ThreadJob(append, (1,))
        job2 = ThreadJob(append, (2,), condition=AfterAnother(job1.id))
        job3 = ThreadJob(append, (3,), condition=AfterAnother(job2.id))
        await engine.submit(job3)
        await engine.submit(job2)
        await engine.submit(job1)
        await engine.wait()
        assert lis == [1, 2, 3]

    asyncio.run(submit_job())


def test_after_others():
    engine = Engine()

    s = set()

    async def submit_job():
        job1 = ThreadJob(lambda: s.add(1))
        job2 = ThreadJob(lambda: s.add(2))
        def has_1_2():
            assert 1 in s
            assert 2 in s
        job3 = ThreadJob(has_1_2, condition=AfterOthers([job1.id, job2.id]))
        await engine.submit(job3)
        await engine.submit(job2)
        await engine.submit(job1)
        await engine.wait()

    asyncio.run(submit_job())


def test_after_timepoint():
    engine = Engine()

    async def submit_job():
        t1 = datetime.now()
        d1 = timedelta(seconds=0.5)
        t2 = t1 + d1
        def assert_after():
            assert datetime.now() > t2
        job = ThreadJob(assert_after, condition=AfterTimepoint(t2))
        await engine.submit(job)
        await engine.wait()

    asyncio.run(submit_job())
