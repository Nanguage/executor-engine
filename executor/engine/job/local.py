from .base import Job


class LocalJob(Job):
    async def run_function(self):
        """Run job in local thread."""
        res = self.func(*self.args, **self.kwargs)
        return res
