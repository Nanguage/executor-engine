from datetime import datetime, timedelta
import asyncio

from executor.engine.core import Engine
from executor.engine.job import LocalJob, ThreadJob, ProcessJob
from executor.engine.job.condition import (
    AfterAnother, AfterOthers, AfterTimepoint,
    AnySatisfied, AllSatisfied
)


def test_after_another_cond():
    engine = Engine()

    lis = []

    def append(x):
        lis.append(x)

    def id_func(x):
        return x

    async def submit_job():
        job1 = LocalJob(id_func, (1,), callback=append)
        job2 = ThreadJob(
            id_func, (2,), callback=append,
            condition=AfterAnother(job_id=job1.id))
        job3 = ProcessJob(
            id_func, (3,), callback=append,
            condition=AfterAnother(job_id=job2.id))
        await engine.submit_async(job3)
        await engine.submit_async(job2)
        await engine.submit_async(job1)
        await engine.join()
        assert lis == [1, 2, 3]
        job4 = ThreadJob(
            id_func, (4,), callback=append,
            condition=AfterAnother(job_id="not_exist"))
        await engine.submit_async(job4)
        await job4.join(timeout=0.1)
        assert len(lis) == 3
        await job4.cancel()

    asyncio.run(submit_job())


def test_after_others_all_mode():
    engine = Engine()

    s = set()

    async def submit_job():
        job1 = ThreadJob(lambda: s.add(1))
        job2 = ThreadJob(lambda: s.add(2))

        def has_1_2():
            assert 1 in s
            assert 2 in s
        job3 = ThreadJob(
            has_1_2,
            condition=AfterOthers(
                job_ids=[job1.id, job2.id],
                mode='all'))
        await engine.submit_async(job3)
        await engine.submit_async(job2)
        await engine.submit_async(job1)
        await engine.join()
        job4 = ThreadJob(
            lambda: s.add(3),
            condition=AfterOthers(job_ids=["not_exist"]))
        await engine.submit_async(job4)
        await engine.join(timeout=0.1)
        assert len(s) == 2
        await job4.cancel()

    asyncio.run(submit_job())


def test_after_others_any_mode():
    engine = Engine()

    s = set()

    async def submit_job():
        job1 = ThreadJob(lambda: s.add(1))
        job2 = ThreadJob(lambda: s.add(2))

        def has_1_or_2():
            assert 1 in s or 2 in s
        job3 = ThreadJob(
            has_1_or_2,
            condition=AfterOthers(
                job_ids=[job1.id, job2.id], mode="any"))
        await engine.submit_async(job3)
        await engine.submit_async(job2)
        await engine.submit_async(job1)
        await engine.join()

    asyncio.run(submit_job())


def test_after_timepoint():
    engine = Engine()

    async def submit_job():
        t1 = datetime.now()
        d1 = timedelta(seconds=0.5)
        t2 = t1 + d1

        def assert_after():
            assert datetime.now() > t2
        job = ThreadJob(assert_after, condition=AfterTimepoint(timepoint=t2))
        await engine.submit_async(job)
        await engine.join()

    asyncio.run(submit_job())


def test_any_satisfy():
    engine = Engine()

    s = set()

    def run_forever():
        while True:
            1 + 1

    async def submit_job():
        job1 = ThreadJob(lambda: s.add(1))
        job2 = ProcessJob(run_forever)

        def has_one_element():
            assert len(s) == 1
        job3 = ThreadJob(has_one_element, condition=AnySatisfied(conditions=[
            AfterAnother(job_id=job1.id),
            AfterAnother(job_id=job2.id)
        ]))
        await engine.submit_async(job3)
        await engine.submit_async(job2)
        await engine.submit_async(job1)
        await engine.join(timeout=1.0)
        await job2.cancel()

    asyncio.run(submit_job())


def test_all_satisfy():
    engine = Engine()

    s = set()

    async def submit_job():
        job1 = ThreadJob(lambda: s.add(1))
        job2 = ThreadJob(lambda: s.add(2))

        def has_two_elements():
            assert len(s) == 2
        job3 = ThreadJob(has_two_elements, condition=AllSatisfied(conditions=[
            AfterAnother(job_id=job1.id),
            AfterAnother(job_id=job2.id)
        ]))
        await engine.submit_async(job3)
        await engine.submit_async(job2)
        await engine.submit_async(job1)
        await engine.join()

    asyncio.run(submit_job())
