import asyncio

from executor.engine.core import Engine
from executor.engine.job.extend.subprocess import SubprocessJob


def test_run_cmd():
    engine = Engine()

    async def submit_job():
        job = SubprocessJob("python -c 'print(1 + 1)'")
        await engine.submit(job)
        await job.join()
        assert job.result() == 0

        job = SubprocessJob("python -c 'print(1 + a)'")
        await engine.submit(job)
        await job.join()
        assert job.result() != 0

    asyncio.run(submit_job())


def test_capture_stdout_stderr():
    engine = Engine()

    async def submit_job():
        job = SubprocessJob("python -c 'print(1 + 1)'", redirect_out_err=True)
        await engine.submit(job)
        await job.join()
        assert job.result() == 0
        with open(job.cache_dir / 'stdout.txt') as f:
            assert f.read() == '2\n'

        job = SubprocessJob("python -c 'print(1 + a)'", redirect_out_err=True)
        await engine.submit(job)
        await job.join()
        assert job.result() != 0
        with open(job.cache_dir / 'stderr.txt') as f:
            assert len(f.read()) > 0

    asyncio.run(submit_job())


def test_record_command():
    engine = Engine()

    async def submit_job():
        cmd = "python -c 'print(1 + 1)'"
        job = SubprocessJob(cmd, record_cmd=True, error_callback=lambda err: print(err))
        await engine.submit(job)
        await job.join()
        assert job.result() == 0
        with open(job.cache_dir / "command.sh") as f:
            assert f.read() == cmd + "\n"

    asyncio.run(submit_job())
