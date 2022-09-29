import datetime
import typing as T

import fire
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel


from ..engine import Engine
from ..job import LocalJob, ThreadJob, ProcessJob
from ..job.base import JobEmitError
from .task import TaskTable


TASK_TABLE = TaskTable()
TASK_TABLE.register(eval)


class CallRequest(BaseModel):
    task_name: str
    args: T.List
    kwargs: T.Dict[str, T.Any]
    job_type: T.Literal["local", "thread", "process"]


def create_app() -> FastAPI:
    app = FastAPI()
    engine = app.engine = Engine()

    @app.post("/call")
    async def call(req: CallRequest):
        try:
            task = TASK_TABLE[req.task_name]
        except KeyError:
            return {"error": "Function not registered."}

        if req.job_type == "local":
            job_cls = LocalJob
        elif req.job_type == "thread":
            job_cls = ThreadJob
        else:
            job_cls = ProcessJob

        job = job_cls(
            task.func, req.args, req.kwargs,
            callback=None,
            error_callback=None,
            name=task.name)
        engine.submit(job)
        return job.to_dict()

    @app.get("/task_list")
    async def get_task_list():
        return list(TASK_TABLE.table.keys())

    @app.get("/job/{job_id}")
    async def get_job_status(job_id: str):
        job = engine.jobs.get_job_by_id(job_id)
        if job:
            return job.to_dict()
        else:
            return {'error': 'Job not found.'}

    @app.get("/jobs")
    async def get_all_jobs():
        resp = []
        for job in engine.jobs.all_jobs():
            resp.append(job.to_dict())
        return resp

    @app.get("/cancel/{job_id}")
    async def cancel_job(job_id: str):
        running = engine.jobs.running
        pending = engine.jobs.pending
        if job_id in running:
            job = running[job_id]
            job.cancel()
            return job.to_dict()
        elif job_id in pending:
            job = pending[job_id]
            job.cancel()
            return job.to_dict()
        else:
            return {"error": "The job is not in running or pending."}

    @app.get("/re_run/{job_id}")
    async def re_run_job(job_id: str):
        job = engine.jobs.get_job_by_id(job_id)
        try:
            job.emit()
            return job.to_dict()
        except JobEmitError as e:
            return {'error': str(e)}

    return app


def run_server(
        host: str = "127.0.0.1",
        port: int = 5000,
        log_level: str = "info",
        **kwargs,
        ):
    config = uvicorn.Config(
        "executor.server.http:create_app", factory=True,
        host=host, port=port,
        log_level=log_level,
        **kwargs
    )
    server = uvicorn.Server(config)
    server.run()


if __name__ == "__main__":
    fire.Fire(run_server)
