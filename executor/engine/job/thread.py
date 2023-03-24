import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor

from .base import Job


class ThreadJob(Job):
    """Job that runs in a thread."""

    def has_resource(self) -> bool:
        """Check if the job has enough resource to run."""
        if self.engine is None:
            return False
        else:
            return (
                super().has_resource() and
                (self.engine.resource.n_thread > 0)
            )

    def consume_resource(self) -> bool:
        """Consume resource for the job."""
        if self.engine is None:
            return False
        else:
            self.engine.resource.n_thread -= 1
            return (
                super().consume_resource() and
                True
            )

    def release_resource(self) -> bool:
        """Release resource for the job."""
        if self.engine is None:
            return False
        else:
            self.engine.resource.n_thread += 1
            return (
                super().release_resource() and
                True
            )

    async def run(self):
        """Run job in thread pool."""
        self._executor = ThreadPoolExecutor(1)
        loop = asyncio.get_running_loop()
        func = functools.partial(self.func, **self.kwargs)
        fut = loop.run_in_executor(self._executor, func, *self.args)
        result = await fut
        return result

    async def cancel(self):
        """Cancel job."""
        if self.status == "running":
            self._executor.shutdown()
        await super().cancel()

    def clear_context(self):
        """Clear context."""
        self._executor.shutdown()
        self._executor = None
