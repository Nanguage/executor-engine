import asyncio
import functools
import inspect

from loky.process_executor import ProcessPoolExecutor

from .base import Job
from .utils import _gen_initializer, GeneratorWrapper


class ProcessJob(Job):
    """Job that runs in a process."""""

    def has_resource(self) -> bool:
        """Check if the job has enough resource to run."""
        if self.engine is None:
            return False
        else:
            return (
                super().has_resource() and
                (self.engine.resource.n_process > 0)
            )

    def consume_resource(self) -> bool:
        """Consume resource for the job."""
        if self.engine is None:
            return False
        else:
            self.engine.resource.n_process -= 1
            return (
                super().consume_resource() and
                True
            )

    def release_resource(self) -> bool:
        """Release resource for the job."""
        if self.engine is None:
            return False
        else:
            self.engine.resource.n_process += 1
            return (
                super().release_resource() and
                True
            )

    async def run(self):
        """Run job in process pool."""
        func = functools.partial(self.func, *self.args, **self.kwargs)
        if (inspect.isgeneratorfunction(self.func)
            or inspect.isasyncgenfunction(self.func)):
            self._executor = ProcessPoolExecutor(1, initializer=_gen_initializer, initargs=(func,))
            result = GeneratorWrapper(self)
        else:
            self._executor = ProcessPoolExecutor(1)
            loop = asyncio.get_running_loop()
            fut = loop.run_in_executor(self._executor, func)
            result = await fut
        return result

    async def cancel(self):
        """Cancel job."""
        if self.status == "running":
            self._executor.shutdown(wait=True, kill_workers=True)
        await super().cancel()

    def clear_context(self):
        """Clear context."""
        self._executor.shutdown(wait=True, kill_workers=True)
        self._executor = None
