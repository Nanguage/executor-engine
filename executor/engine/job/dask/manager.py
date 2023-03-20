from dask.distributed import Client


def get_default_client() -> Client:
    return Client(asynchronous=True)
