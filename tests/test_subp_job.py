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
        job = SubprocessJob("python -c 'print(1 + 1)'", redirect_out_err=True)
        await engine.submit(job)
        await job.join()
        assert job.result() == 0

    asyncio.run(submit_job())
