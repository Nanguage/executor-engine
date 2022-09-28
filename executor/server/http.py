import datetime
import typing as T

import fire
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel


from ..engine import Engine
from ..job import LocalJob, ThreadJob, ProcessJob
from ..job.base import JobEmitError


FUNC_TABLE: T.Dict[str, T.Callable] = {
    "eval": eval
}


class CallRequest(BaseModel):
    func_name: str
    func_args: T.List
    job_type: T.Literal["local", "thread", "process"]


def create_app() -> FastAPI:
    app = FastAPI()
    engine = app.engine = Engine()

    @app.post("/call")
    async def call(req: CallRequest):
        if req.func_name not in FUNC_TABLE:
            return {"error": "Function not registered."}
        func = FUNC_TABLE.get(req.func_name)
        if req.job_type == "local":
            job_cls = LocalJob
        elif req.job_type == "thread":
            job_cls = ThreadJob
        else:
            job_cls = ProcessJob

        job = job_cls(
            func, req.func_args,
            callback=None,
            error_callback=None,
            name=req.func_name)
        engine.submit(job)
        return job.to_dict()

    @app.get("/func_list")
    async def get_func_list():
        return list(FUNC_TABLE.keys())

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
