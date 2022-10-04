import time

from fastapi.testclient import TestClient
import pytest

from executor.server.http import create_app, TASK_TABLE
from executor.server.task import Task

app = create_app()


client = TestClient(app)


@pytest.mark.order(0)
def test_register_task():
    def square(x):
        return x ** 2

    TASK_TABLE.register(Task(square, name='square'))


@pytest.mark.order(1)
def test_list_tasks():
    resp = client.get("/tasks")
    assert resp.status_code == 200
    assert 'square' in [
        t['name'] for t in resp.json()
    ]


@pytest.mark.order(2)
def test_call_task():
    resp = client.post(
        "/call",
        json={
            "task_name": "square",
            "args": [2],
            "kwargs": {},
            "job_type": "thread",
        },
    )
    assert resp.status_code == 200
    assert 'id' in resp.json()


@pytest.mark.order(3)
def test_get_all_jobs():
    resp = client.get("/jobs")
    assert resp.status_code == 200
    assert len(resp.json()) == 1


@pytest.mark.order(4)
def test_cancel_and_rerun_job():
    def add(x, y):
        time.sleep(1)
        return x + y

    TASK_TABLE.register(add)
    resp = client.post(
        "/call",
        json={
            "task_name": "add",
            "args": [1, 2],
            "kwargs": {},
            "job_type": "thread",
        }
    )
    assert resp.status_code == 200
    assert resp.json()['status'] == "running"
    add_id = resp.json()['id']

    resp = client.get(f"/cancel/{add_id}")
    assert resp.status_code == 200
    assert resp.json()['status'] == "canceled"

    resp = client.get(f"/re_run/{add_id}")
    assert resp.status_code == 200
    assert resp.json()['status'] == "running"


@pytest.mark.order(5)
def test_get_job_result():
    def mul(x, y):
        time.sleep(1)
        return x * y
    
    TASK_TABLE.register(mul)
    resp = client.post(
        "/call",
        json={
            "task_name": "mul",
            "args": [40, 2],
            "kwargs": {},
            "job_type": "local",
        }
    )
    assert resp.status_code == 200
    job_id = resp.json()['id']
    resp = client.get(f"/job_result/{job_id}")
    assert resp.status_code == 200
    assert resp.json()['result'] == 80
