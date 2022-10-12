import typing as T

import fire
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from ..engine import Engine
from ..job import Job, LocalJob, ThreadJob, ProcessJob
from ..job.utils import InvalidStateError, JobEmitError
from .task import TaskTable


TASK_TABLE = TaskTable()


class CallRequest(BaseModel):
    task_name: str
    args: T.List
    kwargs: T.Dict[str, T.Any]
    job_type: T.Literal["local", "thread", "process"]


ORIGINS = [
    "http://127.0.0.1",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5000",
    "http://localhost",
]

VALID_JOB_TYPE = ['thread', 'process']


def create_app() -> FastAPI:
    app = FastAPI()
    engine = Engine()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.post("/call")
    async def call(req: CallRequest):
        try:
            task = TASK_TABLE[req.task_name]
        except KeyError:
            return {"error": "Function not registered."}

        job_cls: T.Type["Job"]

        if req.job_type not in VALID_JOB_TYPE:
            return {"error": f"Not valid job type: {req.job_type}"}

        if req.job_type == "local":
            job_cls = LocalJob
        elif req.job_type == "thread":
            job_cls = ThreadJob
        else:
            job_cls = ProcessJob

        job = job_cls(
            task.func, tuple(req.args), req.kwargs,
            callback=None,
            error_callback=None,
            name=task.name)
        await engine.submit(job)
        return job.to_dict()

    @app.get("/tasks")
    async def get_task_list():
        return [t.to_dict() for t in TASK_TABLE.table.values()]

    @app.get("/job/{job_id}")
    async def get_job_status(job_id: str):
        job = engine.jobs.get_job_by_id(job_id)
        if job:
            return job.to_dict()
        else:
            return {'error': 'Job not found.'}

    @app.get("/valid_job_types")
    async def get_valid_job_types():
        return VALID_JOB_TYPE

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
            await job.cancel()
            return job.to_dict()
        elif job_id in pending:
            job = pending[job_id]
            await job.cancel()
            return job.to_dict()
        else:
            return {"error": "The job is not in running or pending."}

    @app.get("/re_run/{job_id}")
    async def re_run_job(job_id: str):
        job = engine.jobs.get_job_by_id(job_id)
        if job is None:
            return {'error': 'Job not found.'}
        try:
            await job.rerun()
            return job.to_dict()
        except JobEmitError as e:
            return {'error': str(e)}

    @app.get("/job_result/{job_id}")
    async def wait_job_result(job_id: str):
        job = engine.jobs.get_job_by_id(job_id)
        if job is None:
            return {'error': 'Job not found.'}
        try:
            await job.join()
            return {
                'job': job.to_dict(),
                'result': job.result(),
            }
        except InvalidStateError:
            return {
                'error': 'Job can not fetch result',
                'job': job.to_dict(),
            }

    return app


def run_server(
        host: str = "127.0.0.1",
        port: int = 5000,
        log_level: str = "info",
        frontend_addr: str = "127.0.0.1:5173",
        valid_job_type: str = "process,thread",
        **kwargs,
        ):
    if frontend_addr not in ORIGINS:
        ORIGINS.append(frontend_addr)
    if valid_job_type:
        global VALID_JOB_TYPE
        VALID_JOB_TYPE = valid_job_type.split(",")
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
