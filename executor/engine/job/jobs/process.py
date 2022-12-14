import asyncio
import functools

from loky.process_executor import ProcessPoolExecutor

from ..base import Job


class ProcessJob(Job):

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
        self.executor = executor = ProcessPoolExecutor(1)
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
            self.executor.shutdown(wait=True, kill_workers=True)
        await super().cancel()

    def clear_context(self):
        self.executor.shutdown(wait=True, kill_workers=True)
        self.executor = None
