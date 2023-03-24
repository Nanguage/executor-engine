from .base import Job


class LocalJob(Job):
    async def run(self):
        """Run job in local thread."""
        res = self.func(*self.args, **self.kwargs)
        return res
