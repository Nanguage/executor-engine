import time
import typing as T
import asyncio

from executor.engine.core import Engine
from executor.engine.job import LocalJob, ThreadJob, ProcessJob, Job
from executor.engine.job.condition import AfterAnother, AnySatisfied


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


def test_cancel_job():
    engine = Engine()

    def run_forever():
        while True:
            1 + 1

    async def submit_job():
        for job_cls in [ProcessJob]:
            job: Job = job_cls(run_forever)
            # cancel running
            await engine.submit(job)
            await asyncio.sleep(0.1)
            await job.cancel()
            assert job.status == "canceled"
            # cancel after re-run
            await job.rerun()
            await asyncio.sleep(0.1)
            assert job.status == "running"
            await job.cancel()
            # cancel pending
            await job.rerun()
            assert job.status == "pending"
            await job.cancel()

    asyncio.run(submit_job())


def test_re_run_job():
    engine = Engine()

    async def submit_job():
        for job_cls in test_job_cls:
            job: Job = job_cls(lambda x: x**2, (2,))
            await engine.submit(job)
            await job.join()
            assert job.status == "done"
            await job.rerun()
            await job.join()
            assert job.status == "done"

    asyncio.run(submit_job())


def test_delete_job():
    engine = Engine()

    def sleep_1s():
        time.sleep(1)

    async def submit_job():
        for job_cls in test_job_cls:
            # remove pending job
            job: Job = job_cls(lambda x: x**2, (2,))
            await engine.submit(job)
            assert job in engine.jobs
            assert job.status == "pending"
            await engine.remove(job)
            assert job not in engine.jobs

        for job_cls in [ThreadJob, ProcessJob]:
            # remove running job
            job: Job = job_cls(sleep_1s)
            await engine.submit(job)
            assert job in engine.jobs
            await asyncio.sleep(0.1)
            assert job.status == "running"
            await engine.remove(job)
            assert job not in engine.jobs

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


def test_repr_job():
    def print_hello():
        print("hello")

    job_cls: T.Type[Job]
    for job_cls in test_job_cls:
        job1 = job_cls(print_hello)
        str(job1)
        repr(job1)
        job2 = job_cls(print_hello, condition=AfterAnother(job_id=job1.id))
        str(job2)
        repr(job2)
        job3 = job_cls(print_hello, condition=AnySatisfied(
            conditions=[AfterAnother(job_id=job1.id), AfterAnother(job_id=job2.id)]))
        str(job3)
        repr(job3)
