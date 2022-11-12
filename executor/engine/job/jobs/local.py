from ..base import Job


class LocalJob(Job):
    async def run(self):
        success = False
        try:
            res = self.func(*self.args, **self.kwargs)
            success = True
        except Exception as e:
            await self.on_failed(e)
        if success:
            await self.on_done(res)
            return res
