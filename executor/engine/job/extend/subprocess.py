import copy
import shlex
import asyncio
import typing as T
import subprocess as subp

from ..condition import Condition
from ..jobs.process import ProcessJob
from ...utils import ProcessRunner


class SubprocessJob(ProcessJob):
    def __init__(
            self,
            cmd: str,
            callback: T.Optional[T.Callable[[T.Any], None]] = None,
            error_callback: T.Optional[T.Callable[[Exception], None]] = None,
            name: T.Optional[str] = None,
            condition: T.Optional[Condition] = None,
            time_delta: float = 0.01,
            redirect_out_err: bool = False,
            **attrs
            ) -> None:
        self.cmd = cmd
        if name is None:
            name = cmd.split()[0]
        super().__init__(
            lambda x: x,
            callback=callback,
            error_callback=error_callback,
            name=name,
            condition=condition,
            time_delta=time_delta,
            redirect_out_err=redirect_out_err,
            **attrs
        )

    def __repr__(self) -> str:
        attrs = [
            f"status={self.status}",
            f"id={self.id}",
            f"cmd={self.cmd}",
        ]
        if self.condition:
            attrs.append(f" condition={self.condition}")
        attr_str = " ".join(attrs)
        return f"<{self.__class__.__name__} {attr_str}/>"

    def process_func(self):
        cmd = copy.copy(self.cmd)
        if self.redirect_out_err:
            path_stdout = self.cache_dir / 'stdout.txt'
            path_stderr = self.cache_dir / 'stderr.txt'
            def func():
                runner = ProcessRunner(cmd)
                runner.run()
                g = runner.stream()
                with open(path_stdout, 'w') as fo, open(path_stderr, 'w') as fe:
                    while True:
                        try:
                            src, line = next(g)
                            if src == 'stdout':
                                fo.write(line.rstrip("\n"))
                            else:
                                fe.write(line.rstrip("\n"))
                        except StopIteration as e:
                            retcode = e.value
                            break
                return retcode
        else:
            def func():
                p = subp.Popen(shlex.split(cmd))
                retcode = p.wait()
                return retcode
        self.func = func
