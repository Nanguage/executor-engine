import typing as T


class Task(object):
    def __init__(
            self, func: T.Callable,
            name: T.Optional[str] = None,
            description: str = "",
            **kwargs
            ):
        self.func = func
        self.name = name or func.__name__
        self.description = description
        self.attrs = kwargs

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "attrs": self.attrs,
        }


class TaskTable(object):
    def __init__(self, table: T.Optional[T.Dict[str, Task]] = None) -> None:
        self.table: T.Dict[str, Task] = table or {}

    def __getitem__(self, key) -> Task:
        return self.table[key]

    def register(self, task: T.Union[Task, T.Callable]):
        if not isinstance(task, Task):
            task = Task(task)
        self.table[task.name] = task


