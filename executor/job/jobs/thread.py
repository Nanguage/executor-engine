import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor

from ..base import Job


class ThreadJob(Job):
    job_type: str = "thread"

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
        exe = self.executor = ThreadPoolExecutor(1)
        loop = asyncio.get_running_loop()
        func = functools.partial(self.func, **self.kwargs)
        try:
            result = await loop.run_in_executor(exe, func, *self.args)
            await self.on_done(result)
            return result
        except Exception as e:
            await self.on_failed(e)

    def clear_context(self):
        self.executor.shutdown()
        del self.executor
