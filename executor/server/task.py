import typing as T
from oneface.arg import parse_func_args, Empty


class Task(object):
    def __init__(
            self, func: T.Callable,
            name: T.Optional[str] = None,
            description: str = "",
            **kwargs
            ):
        self.func = func
        self.name = name or func.__name__
        if not description:
            if hasattr(func, "__doc__") and (func.__doc__ is not None):
                description = func.__doc__
        self.description = description
        self.func_args = self.get_func_args()
        self.attrs = kwargs

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "args": self.func_args,
            "attrs": self.attrs,
        }

    def get_func_args(self) -> T.List[dict]:
        """Return JSON serializable argument information for frontend."""
        args = []
        arg_objs = parse_func_args(self.func)
        for name, o in arg_objs.items():
            default = None if o.default is Empty else o.default
            if o.type is Empty:
                o.type = str  # set default type to str
            type_name = str(o.type)
            if hasattr(o.type, "__name__"):
                type_name = o.type.__name__
            arg = {
                "name": name,
                "type": type_name,
                "range": o.range,
                "default": default,
            }
            args.append(arg)
        return args


class TaskTable(object):
    def __init__(self, table: T.Optional[T.Dict[str, Task]] = None) -> None:
        self.table: T.Dict[str, Task] = table or {}

    def __getitem__(self, key) -> Task:
        return self.table[key]

    def register(self, task: T.Union[Task, T.Callable]):
        if not isinstance(task, Task):
            task = Task(task)
        self.table[task.name] = task
