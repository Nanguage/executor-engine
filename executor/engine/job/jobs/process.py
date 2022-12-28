import asyncio
import functools

from loky import get_reusable_executor

from ..base import Job


class ProcessJob(Job):
    MAX_WORKERS = 8

    def has_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            return self.engine.process_count > 0

    def consume_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.process_count -= 1
            return True

    def release_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.process_count += 1
            return True

    async def run(self):
        executor = get_reusable_executor(
            max_workers=ProcessJob.MAX_WORKERS)
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
        self.fut.cancel()
        self.fut = None
