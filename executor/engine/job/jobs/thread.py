import typing as T
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor

from ..base import Job


_executor: T.Optional[ThreadPoolExecutor] = None

def get_reuseable_executor(max_worker: int = 8) -> ThreadPoolExecutor:
    global _executor
    if (_executor is None) or (_executor._max_workers != max_worker):
        _executor = ThreadPoolExecutor(max_workers=max_worker)
    return _executor


class ThreadJob(Job):
    MAX_WORKERS = 8

    def has_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            return self.engine.thread_count > 0

    def consume_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.thread_count -= 1
            return True

    def release_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.thread_count += 1
            return True

    async def run(self):
        executor = get_reuseable_executor(max_worker=ThreadJob.MAX_WORKERS)
        loop = asyncio.get_running_loop()
        func = functools.partial(self.func, **self.kwargs)
        try:
            self.fut = fut = loop.run_in_executor(executor, func, *self.args)
            result = await fut
            await self.on_done(result)
            return result
        except Exception as e:
            await self.on_failed(e)

    async def cancel(self):
        if self.status == "running":
            self.fut.cancel()
        await super().cancel()

    def clear_context(self):
        self.fut = None
