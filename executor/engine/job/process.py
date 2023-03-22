import asyncio
import functools

from loky.process_executor import ProcessPoolExecutor

from .base import Job


class ProcessJob(Job):

    def has_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            return (
                super().has_resource() and
                (self.engine.resource.n_process > 0)
            )

    def consume_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.resource.n_process -= 1
            return (
                super().consume_resource() and
                True
            )

    def release_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.resource.n_process += 1
            return (
                super().release_resource() and
                True
            )

    async def run(self):
        self._executor = ProcessPoolExecutor(1)
        loop = asyncio.get_running_loop()
        func = functools.partial(self.func, **self.kwargs)
        fut = loop.run_in_executor(self._executor, func, *self.args)
        result = await fut
        return result

    async def cancel(self):
        if self.status == "running":
            self._executor.shutdown(wait=True, kill_workers=True)
        await super().cancel()

    def clear_context(self):
        self._executor.shutdown(wait=True, kill_workers=True)
        self._executor = None