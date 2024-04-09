import asyncio

from executor.engine.core import Engine, EngineSetting
from executor.engine.job.extend.subprocess import SubprocessJob
from executor.engine.job import ProcessJob, LocalJob
from executor.engine.job.dask import DaskJob
from executor.engine.job.condition import AfterAnother


def test_run_cmd():
    engine = Engine()

    async def submit_job():
        job = SubprocessJob("python -c 'print(1 + 1)'")
        await engine.submit_async(job)
        await job.join()
        assert job.status == "done"
        assert job.result() == 0

        job = SubprocessJob("python -c 'print(1 + a)'")
        await engine.submit_async(job)
        await job.join()
        assert job.status == "failed"

    asyncio.run(submit_job())


def test_capture_stdout_stderr():
    engine = Engine(
        setting=EngineSetting(print_traceback=False)
    )

    async def submit_job():
        job = SubprocessJob("python -c 'print(1 + 1)'", redirect_out_err=True)
        await engine.submit_async(job)
        await job.join()
        assert job.result() == 0
        with open(job.cache_dir / 'stdout.txt') as f:
            assert f.read().strip() == '2'

        job = SubprocessJob("python -c 'print(1 + a)'", redirect_out_err=True)
        await engine.submit_async(job)
        await job.join()
        assert job.status == "failed"
        with open(job.cache_dir / 'stderr.txt') as f:
            assert len(f.read()) > 0

    asyncio.run(submit_job())


def test_record_command():
    engine = Engine()

    async def submit_job():
        cmd = "python -c 'print(1 + 1)'"
        job = SubprocessJob(
            cmd, record_cmd=True,
            error_callback=lambda err: print(err))
        await engine.submit_async(job)
        await job.join()
        assert job.result() == 0
        with open(job.cache_dir / "command.sh") as f:
            assert f.read() == cmd + "\n"

    asyncio.run(submit_job())


def test_condition():
    engine = Engine()

    lis = []

    def append(x):
        lis.append(x)

    async def submit_job():
        cmd = "python -c 'print(1 + 1)'"
        job1 = SubprocessJob(cmd, callback=lambda _: append(1), )
        job2 = SubprocessJob(
            cmd, callback=lambda _: append(2),
            condition=AfterAnother(job_id=job1.id))
        job3 = SubprocessJob(
            cmd, callback=lambda _: append(3),
            condition=AfterAnother(job_id=job2.id))
        await engine.submit_async(job3)
        await engine.submit_async(job2)
        await engine.submit_async(job1)
        await engine.join()
        assert lis == [1, 2, 3]

    asyncio.run(submit_job())


def test_repr():
    job = SubprocessJob("python -c 'print(1 + 1)'")
    assert job.cmd in repr(job)
    job1 = SubprocessJob(
        "python -c 'print(1 + 1)'",
        condition=AfterAnother(job_id=job.id)
    )
    assert repr(job1.condition) in repr(job1)


def test_based_on_other_type():
    engine = Engine()

    with engine:
        for base_class in [ProcessJob, DaskJob, LocalJob]:
            job = SubprocessJob(
                "python -c 'print(1 + 1)'",
                base_class=base_class)
            engine.submit(job)
            engine.wait_job(job)
            assert job.status == "done"
            assert job.result() == 0


def test_target_dir():
    engine = Engine()

    async def submit_job():
        job1 = SubprocessJob(
            "python -c 'print(1 + 1)'",
            target_dir="$cache_dir")
        await engine.submit_async(job1)
        job2 = SubprocessJob(
            "python -c 'print(1 + 1)'",
            target_dir=f"{job1.cache_dir}")
        await engine.submit_async(job2)
        await engine.join()
        assert job1.status == "done"
        assert job2.status == "done"
        assert job1.result() == 0
        assert job2.result() == 0

    asyncio.run(submit_job())


def test_passing_env():
    engine = Engine()

    async def submit_job():
        import os
        env = os.environ.copy()
        env["A"] = "2"
        job = SubprocessJob(
            "python -c 'import os; print(os.getenv(\"A\"))'",
            popen_kwargs={"env": env},
            redirect_out_err=True
        )
        await engine.submit_async(job)
        await job.join()
        with open(job.cache_dir / 'stdout.txt') as f:
            assert f.read().strip() == '2'

    asyncio.run(submit_job())


def test_cancel():
    engine = Engine()

    async def submit_job():
        job = SubprocessJob("python -c 'import time; time.sleep(10)'")
        await engine.submit_async(job)
        await asyncio.sleep(1)
        await job.cancel()
        await job.join()
        assert job.status == "cancelled"

    asyncio.run(submit_job())
