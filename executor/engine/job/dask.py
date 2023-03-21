import functools

from dask.distributed import Client

from .base import Job


def get_default_client() -> Client:
    return Client(asynchronous=True)


class DaskJob(Job):
    def has_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            return (
                super().has_resource() and
                (self.engine.resource.n_dask > 0)
            )

    def consume_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.resource.n_dask -= 1
            return (
                super().consume_resource() and
                True
            )

    def release_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.resource.n_dask += 1
            return (
                super().release_resource() and
                True
            )

    async def run(self):
        client = self.engine.dask_client
        func = functools.partial(self.func, **self.kwargs)
        fut = client.submit(func, *self.args)
        self._executor = fut
        result = await fut
        return result

    async def cancel(self):
        if self.status == "running":
            await self._executor.cancel()
        await super().cancel()

    def clear_context(self):
        self._executor = None
