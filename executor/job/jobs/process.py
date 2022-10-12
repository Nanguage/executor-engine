import asyncio
import functools

from loky import get_reusable_executor

from ..base import Job


class ProcessJob(Job):
    job_type: str = "process"

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
        self.executor = executor = get_reusable_executor(max_workers=1)
        loop = asyncio.get_running_loop()
        func = functools.partial(self.func, **self.kwargs)
        try:
            result = await loop.run_in_executor(executor, func, *self.args)
            await self.on_done(result)
        except Exception as e:
            await self.on_failed(e)
        return result

    def clear_context(self):
        self.executor.shutdown()
        del self.executor
