from executor.engine.task import task
import time


def test_task_and_fut():
    @task(job_type='process')
    def add(a, b):
        time.sleep(1)
        return a + b

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
