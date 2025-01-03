import pytest
import asyncio
from datetime import datetime, timedelta

from executor.engine import Engine
from executor.engine.job.extend.cron import (
    EveryPeriod, CronJob,
    between_clock, between_weekday, between_timepoint
)
from executor.engine.job.extend.sentinel import SentinelJob
from executor.engine.job import LocalJob


@pytest.mark.asyncio
async def test_sentinel_job():
    with Engine() as engine:
        a = 0

        async def inc():
            nonlocal a
            a += 1

        job = CronJob(inc, EveryPeriod("1s"), job_type="thread")
        await engine.submit_async(job)
        await asyncio.sleep(6)
        await job.cancel()
        assert a == 5

        job1 = CronJob(
            lambda: print("hello"), EveryPeriod("0.5s"), job_type=LocalJob)
        job2 = SentinelJob(
            lambda: print("world"), EveryPeriod("0.5s"), job_type="process")
        job3 = SentinelJob(
            lambda: print("world"), EveryPeriod("0.3s"), job_type="local")
        await engine.submit_async(job1, job2, job3)
        await asyncio.sleep(2)
        await job1.cancel()
        await job2.cancel()
        await job3.cancel()
        assert a == 5


def test_conditions():
    assert between_clock("00:00:00", "23:59:59").satisfy(None)
    assert between_weekday("mon", "sun").satisfy(None)
    assert between_timepoint(
        datetime.now() - timedelta(hours=1),
        datetime.now() + timedelta(hours=1)
    ).satisfy(None)
