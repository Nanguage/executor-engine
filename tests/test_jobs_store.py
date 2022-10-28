import asyncio
import traceback

from executor.engine.core import Engine
from executor.engine.job import LocalJob, ThreadJob, ProcessJob
from executor.engine.manager import Jobs


test_job_cls = [LocalJob, ThreadJob, ProcessJob]


def test_jobs_cache():
    engine = Engine()

    async def submit_job():
        for job_cls in test_job_cls:
            job = job_cls(
                lambda x: x**2, (2,),
                error_callback=lambda err: traceback.print_exc())
            await engine.submit(job)
        await engine.wait()

    asyncio.run(submit_job())
    assert len(engine.jobs.done.cache) == 3

    old_path = engine.jobs.cache_path
    jobs = Jobs(old_path)
    assert len(jobs.done.cache) == 3
    jobs.update_from_cache()
    assert len(jobs.done.cache) == 3
