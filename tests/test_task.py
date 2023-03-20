import time
import asyncio

from executor.engine.task import task


def test_task_and_fut():
    @task(job_type='process')
    def add(a, b):
        time.sleep(1)
        return a + b

    assert add.async_mode is False

    fut = add.submit(1, 2)
    assert fut.done() is False
    assert fut.running() is True
    assert fut.result() == 3

    fut = add.submit(1, 2)
    fut.set_result(4)
    assert fut.result() == 4

    fut = add.submit(1, 2)
    fut.cancel()
    assert fut.cancelled() is False

    fut = add.submit(1, 2)
    e = Exception('test')
    fut.set_exception(e)
    assert fut.exception() is e

    fut = add.submit(1, 1)
    var = 1
    def set_var(fut):
        nonlocal var
        var = fut.result()
    fut.add_done_callback(lambda x: set_var(x))
    fut.result()
    assert var == 2


def test_sync_chain():
    @task
    def add(a, b):
        return a + b

    fut = add.submit(1, 2)
    fut2 = add.submit(fut, 2)
    assert fut2.result() == 5
    fut1 = add.submit(1, 2)
    fut2 = add.submit(a=fut1.result(), b=fut1.result())
    assert fut2.result() == 6


def test_async_task_run():
    @task(async_mode=True)
    def add(a, b):
        time.sleep(0.5)
        return a + b

    assert add.async_mode is True

    async def main():
        a = await add(1, 2)
        b = await add(a, 2)
        assert b == 5

    asyncio.run(main())


def test_sync_async_convert():
    @task(async_mode=True)
    def add(a, b):
        time.sleep(0.5)
        return a + b

    assert add.async_mode is True
    add_sync = add.to_sync()
    assert add_sync.async_mode is False
    add_async = add_sync.to_async()
    assert add_async.async_mode is True
