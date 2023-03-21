from .base import Job


class LocalJob(Job):
    async def run(self):
        res = self.func(*self.args, **self.kwargs)
        return res
