from fastapi.testclient import TestClient

from executor.server.http import create_app

app = create_app()


client = TestClient(app)


def test_list_tasks():
    resp = client.get("/task_list")
    assert resp.status_code == 200


def test_call_task():
    resp = client.post(
        "/call",
        {
            "task_name": "eval",
            "args": ["'1'"],
            "kwargs": {},
            "job_type": "thread",
        },
    )
    assert resp.status_code == 200
