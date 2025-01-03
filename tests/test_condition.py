import pytest

from datetime import datetime, timedelta
import asyncio

from executor.engine.core import Engine
from executor.engine.job import LocalJob, ThreadJob, ProcessJob
from executor.engine.job.condition import (
    AfterAnother, AfterOthers,
    AnySatisfied, AllSatisfied,
    _parse_period_str,
    _parse_weekday_str,
    _parse_clock_str,
    AfterClock,
    BeforeClock,
    AfterTimepoint,
    BeforeTimepoint,
    AfterWeekday,
    BeforeWeekday,
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


def test_condition_operators():
    assert isinstance(
        AfterAnother(job_id="1") | AfterAnother(job_id="2"),
        AnySatisfied
    )
    assert isinstance(
        AfterAnother(job_id="1") & AfterAnother(job_id="2"),
        AllSatisfied
    )


def test_parse_period_str():
    assert _parse_period_str("1d").days == 1
    assert _parse_period_str("1.5h").seconds == 5400
    assert _parse_period_str("1m").seconds == 60
    assert _parse_period_str("1s").seconds == 1

    with pytest.raises(ValueError):
        _parse_period_str("1")


def test_parse_weekday_str():
    assert _parse_weekday_str("mon") == 0
    assert _parse_weekday_str("tue") == 1
    assert _parse_weekday_str("wed") == 2
    assert _parse_weekday_str("thu") == 3
    assert _parse_weekday_str("fri") == 4
    assert _parse_weekday_str("sat") == 5
    assert _parse_weekday_str("sun") == 6

    with pytest.raises(ValueError):
        _parse_weekday_str("not_exist")


def test_parse_clock_str():
    assert _parse_clock_str("12") == (12, 0, 0)
    assert _parse_clock_str("12:00") == (12, 0, 0)
    assert _parse_clock_str("12:00:00") == (12, 0, 0)

    with pytest.raises(ValueError):
        _parse_clock_str("not_exist")


def test_time_condition():
    assert AfterClock("00:00:00").satisfy(None)
    assert BeforeClock("23:59:59").satisfy(None)
    assert AfterTimepoint(datetime.now() - timedelta(hours=1)).satisfy(None)
    assert BeforeTimepoint(datetime.now() + timedelta(hours=1)).satisfy(None)
    assert AfterTimepoint(
        datetime.now() + timedelta(hours=1), compare_fields=["hour"]
    ).satisfy(None)
    assert BeforeTimepoint(
        datetime.now() - timedelta(hours=1), compare_fields=["hour"]
    ).satisfy(None)
    assert not AfterTimepoint(
        datetime.now() - timedelta(hours=1), compare_fields=["hour"]
    ).satisfy(None)
    assert not BeforeTimepoint(
        datetime.now() + timedelta(hours=1), compare_fields=["hour"]
    ).satisfy(None)
    assert AfterWeekday("mon").satisfy(None)
    assert BeforeWeekday("sun").satisfy(None)

    with pytest.raises(ValueError):
        AfterTimepoint(
            datetime.now(), compare_fields=["not_exist"]).satisfy(None)

    with pytest.raises(ValueError):
        BeforeTimepoint(
            datetime.now(), compare_fields=["not_exist"]).satisfy(None)
