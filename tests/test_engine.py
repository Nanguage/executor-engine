import typing as T
import asyncio
from datetime import datetime, timedelta

from executor.engine.core import Engine
from executor.engine.job import LocalJob, ThreadJob, ProcessJob, Job
from executor.engine.job.condition import AfterAnother, AfterOthers, AfterTimepoint


test_job_cls = [LocalJob, ThreadJob, ProcessJob]


def test_submit_job():
    engine = Engine()

    n_run = 0
    def callback(res):
        nonlocal n_run
        n_run += 1
        assert res == 4

    async def submit_job():
        for job_cls in test_job_cls:
            job = job_cls(lambda x: x**2, (2,), callback=callback)
            await engine.submit(job)
        await engine.wait()

    asyncio.run(submit_job())
    assert n_run == 3


def test_get_job_result():
    engine = Engine()

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


def test_capture_stdout_stderr():
    engine = Engine()

    def print_hello():
        print("hello")

    def raise_exception():
        raise ValueError("error")

    def read_hello(job: Job):
        with open(job.cache_dir / 'stdout.txt') as f:
            assert f.read() == "hello\n"

    def read_stderr(job: Job):
        with open(job.cache_dir / 'stderr.txt') as f:
            assert len(f.read()) > 0

    def on_failed(err):
        print(err)

    async def submit_job():
        job_cls: T.Type[Job]
        for job_cls in test_job_cls:
            job = job_cls(
                print_hello, redirect_out_err=True,
                error_callback=on_failed)
            await engine.submit(job)
            await job.join()
            assert job.status == "done"
            read_hello(job)

            job = job_cls(
                raise_exception, redirect_out_err=True,
            )
            await engine.submit(job)
            await job.join()
            assert job.status == "failed"
            read_stderr(job)

    asyncio.run(submit_job())
