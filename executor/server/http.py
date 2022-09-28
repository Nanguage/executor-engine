import typing as T

import fire
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from ..engine import Engine
from ..job import LocalJob, ThreadJob, ProcessJob


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

    @app.get("/")
    async def root():
        return {"message": "Hello World!"}

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

        def callback(res):
            job.res = res

        def error_callback(e):
            job.e = e

        job = job_cls(
            func, req.func_args,
            callback=callback,
            error_callback=error_callback)
        engine.submit(job)
        job.join()
        if hasattr(job, "res"):
            return {"result": job.res}
        elif hasattr(job, "e"):
            return {"error": str(e)}
        else:
            return "done"

    @app.get("/func_list")
    async def get_func_list():
        return list(FUNC_TABLE.keys())

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