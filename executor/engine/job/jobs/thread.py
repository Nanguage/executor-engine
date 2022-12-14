import typing as T
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor

from ..base import Job


class ThreadJob(Job):

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
        self.executor = executor = ThreadPoolExecutor(1)
        loop = asyncio.get_running_loop()
        func = functools.partial(self.func, **self.kwargs)
        try:
            fut = loop.run_in_executor(executor, func, *self.args)
            result = await fut
            await self.on_done(result)
            return result
        except Exception as e:
            await self.on_failed(e)

    async def cancel(self):
        if self.status == "running":
            self.executor.shutdown()
        await super().cancel()

    def clear_context(self):
        self.executor.shutdown()
        self.executor = None
