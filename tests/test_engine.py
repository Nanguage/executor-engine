import time
import typing as T
import shutil
import asyncio

import pytest

from executor.engine.core import Engine, EngineSetting
from executor.engine.job import LocalJob, ThreadJob, ProcessJob, Job
from executor.engine.job.condition import AfterAnother, AnySatisfied


test_job_cls = [LocalJob, ThreadJob, ProcessJob]


def test_submit_job():
    n_run = 0

    def callback(res):
        nonlocal n_run
        n_run += 1
        assert res == 4

    with Engine() as engine:
        for job_cls in test_job_cls:
            job = job_cls(lambda x: x**2, (2,), callback=callback)
            engine.submit(job)

        engine.wait()

    assert n_run == 3


def test_err_callback():
    def raise_err():
        raise ValueError("test")

    n_run = 0

    def err_callback(e):
        nonlocal n_run
        n_run += 1
        print(e)

    setting = EngineSetting(print_traceback=False)
    with Engine(setting) as engine:
        for job_cls in test_job_cls:
            job = job_cls(
                raise_err,
                error_callback=err_callback,
            )
            engine.submit(job)
        engine.wait()

    assert n_run == 3


def test_get_job_result():
    with Engine() as engine:
        for job_cls in test_job_cls:
            job: Job = job_cls(lambda x: x**2, (2,))
            engine.submit(job)
            res = engine.wait_job(job)
            assert res == 4


def test_parallel():
    def sleep_add(a):
        time.sleep(3)
        return a + 1

    with Engine() as engine:
        j1 = ProcessJob(sleep_add, (1,))
        j2 = ProcessJob(sleep_add, (2,))
        t1 = time.time()
        engine.submit(j1, j2)
        engine.wait()
        t2 = time.time()
        assert (t2 - t1) < 5


def run_forever():
    while True:
        1 + 1


def test_cancel_job():
    with Engine() as engine:
        for job_cls in [ProcessJob]:
            job: Job = job_cls(run_forever)
            # cancel running
            engine.submit(job)
            engine.cancel(job)
            assert job.status == "cancelled"


def test_cancel_pending():
    setting = EngineSetting(max_jobs=1)
    with Engine(setting=setting) as engine:
        # cancel pending
        job1 = ProcessJob(run_forever)
        job2 = ProcessJob(run_forever)
        engine.submit(job1)
        assert job1.status == "running"
        engine.submit(job2)
        assert job2.status == "pending"
        engine.cancel(job2)
        assert job2.status == "cancelled"
        engine.cancel(job1)
        assert job1.status == "cancelled"


def test_cancel_all():
    # test engine.cancel_all
    with Engine() as engine:
        for job_cls in [ProcessJob]:
            for _ in range(3):
                job: Job = job_cls(run_forever)
                engine.submit(job)
                time.sleep(0.1)
                assert job.status == "running"
        engine.cancel_all()
        for job in engine.jobs:
            assert job.status == "cancelled"


def test_re_submit_job():
    with Engine() as engine:
        for job_cls in test_job_cls:
            job: Job = job_cls(lambda x: x**2, (2,))
            engine.submit(job)
            engine.wait_job(job)
            assert job.status == "done"
            engine.submit(job)
            engine.wait_job(job)
            assert job.status == "done"


def test_remove_job():

    def sleep_1s():
        time.sleep(1)

    with Engine() as engine:
        for job_cls in [ThreadJob, ProcessJob]:
            # remove running job
            job: Job = job_cls(sleep_1s)
            engine.submit(job)
            assert job in engine.jobs
            assert job.status == "running"
            engine.remove(job)
            assert job not in engine.jobs


def test_capture_stdout_stderr():
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

    with Engine() as engine:
        job_cls: T.Type[Job]
        for job_cls in test_job_cls:
            job = job_cls(
                print_hello, redirect_out_err=True,
                error_callback=on_failed)
            engine.submit(job)
            engine.wait_job(job)
            assert job.status == "done"
            read_hello(job)

            job = job_cls(
                raise_exception, redirect_out_err=True,
            )
            engine.submit(job)
            engine.wait_job(job)
            assert job.status == "failed"
            assert isinstance(job.exception(), ValueError)
            read_stderr(job)


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
            conditions=[
                AfterAnother(job_id=job1.id),
                AfterAnother(job_id=job2.id)
            ]))
        str(job3)
        repr(job3)


def test_chdir():
    def write_file():
        with open("1.txt", 'w') as f:
            f.write("111")

    def write_file2():
        with open("2.txt", 'w') as f:
            f.write("222")

    with Engine() as engine:
        job_cls: T.Type[Job]
        for job_cls in test_job_cls:
            job = job_cls(write_file, change_dir=True)
            engine.submit(job)
            engine.wait_job(job)
            with open(job.cache_dir / '1.txt') as f:
                assert f.read() == "111"
            job = job_cls(write_file2, change_dir=True)
            engine.submit(job)
            engine.wait_job(job)
            with open(job.cache_dir / '2.txt') as f:
                assert f.read() == "222"


def test_engine_get_cache_dir():
    engine = Engine()
    p = engine.get_cache_dir()
    assert p == engine.get_cache_dir()
    setting = engine.setting
    setting.cache_path = "./test_cache"
    del engine
    engine = Engine(setting=setting)
    engine.get_cache_dir()
    del engine
    shutil.rmtree(setting.cache_path)


@pytest.mark.asyncio
async def test_async_api():
    engine = Engine()
    job1 = ThreadJob(lambda x: x**2, (2,))
    job2 = ThreadJob(lambda x: x**2, (2,))
    await engine.submit_async(job1, job2)
    await engine.join()
    assert job1.status == "done"
    assert job2.result() == 4

    def sleep_5s():
        time.sleep(5)

    job5 = ProcessJob(sleep_5s)
    await engine.submit_async(job5)
    await engine.wait_async(timeout=1.0)
    assert job5.status == "running"
    await engine.cancel_all_async()


@pytest.mark.asyncio
async def test_join_jobs():
    engine = Engine()
    job1 = ThreadJob(lambda x: x**2, (2,))
    job2 = ThreadJob(lambda x: x**2, (3,))
    await engine.submit_async(job1, job2)
    await engine.join(jobs=[job1, job2])
    assert job1.status == "done"
    assert job2.result() == 9


@pytest.mark.asyncio
async def test_wait_jobs():
    def sleep_square(x):
        time.sleep(x)
        return x**2
    engine = Engine()
    job1 = ThreadJob(sleep_square, (1,))
    job2 = ThreadJob(sleep_square, (2,))
    job3 = ThreadJob(sleep_square, (3,))
    await engine.submit_async(job1, job2, job3)

    def select_func(jobs):
        if (job1.status == "running") or (job1.status == "pending"):
            return [job1]
        else:
            return []
    await engine.wait_async(
        select_jobs=select_func
    )
    assert job1.status == "done"
    assert job2.status == "running"
    assert job3.status == "running"
    await engine.wait_async()


def test_engine_start_stop():
    engine = Engine()
    engine.start()
    engine.start()  # start twice, will warning
    engine.stop()
    engine = Engine()
    engine.stop()  # loop is None, will warning
    engine.start()
    engine.stop()
    engine.stop()  # stop twice, will warning


def test_wait_timeout():
    def sleep_10s():
        time.sleep(10)

    with Engine() as engine:
        job = ProcessJob(sleep_10s)
        engine.submit(job)
        t = time.time()
        engine.wait(timeout=1.0)
        t2 = time.time()
        assert (t2 - t) < 2.0
        engine.cancel_all()


def test_corner_case():
    job = ProcessJob(lambda x: x**2, (2,))
    engine = Engine()
    with pytest.raises(RuntimeError):
        engine.submit(job)
    engine.loop = asyncio.new_event_loop()
