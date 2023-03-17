import os
import copy
import shlex
import typing as T
import subprocess as subp

from ..condition import Condition
from ..jobs.process import ProcessJob

from cmd2func.runner import ProcessRunner


class SubprocessJob(ProcessJob):
    def __init__(
            self,
            cmd: str, record_cmd: bool = True,
            callback: T.Optional[T.Callable[[T.Any], None]] = None,
            error_callback: T.Optional[T.Callable[[Exception], None]] = None,
            name: T.Optional[str] = None,
            condition: T.Optional[Condition] = None,
            time_delta: float = 0.01,
            redirect_out_err: bool = False,
            change_dir: bool = True,
            **attrs
            ) -> None:
        self.cmd = cmd
        self.record_cmd = record_cmd
        if name is None:
            name = cmd.split()[0]
        attrs.update({
            'cmd': cmd,
        })
        super().__init__(
            lambda x: x,
            callback=callback,
            error_callback=error_callback,
            name=name,
            condition=condition,
            time_delta=time_delta,
            redirect_out_err=redirect_out_err,
            change_dir=change_dir,
            **attrs
        )

    def __repr__(self) -> str:
        attrs = [
            f"status={self.status}",
            f"id={self.id}",
            f"cmd={self.cmd}",
        ]
        if self.condition:
            attrs.append(f" condition={repr(self.condition)}")
        attr_str = " ".join(attrs)
        return f"<{self.__class__.__name__} {attr_str}/>"

    def process_func(self):
        cmd = copy.copy(self.cmd)
        record_cmd = copy.copy(self.record_cmd)
        change_dir = copy.copy(self.change_dir)

        cache_dir = self.cache_dir.resolve()
        path_sh = cache_dir / 'command.sh'
        work_dir = cache_dir.resolve()

        def record_command():
            with open(path_sh, 'w') as f:
                f.write(cmd + "\n")

        if self.redirect_out_err:
            path_stdout = cache_dir / 'stdout.txt'
            path_stderr = cache_dir / 'stderr.txt'

            def run_cmd():  # pragma: no cover
                runner = ProcessRunner(cmd)
                runner.run()
                with open(path_stdout, 'w') as fo, \
                     open(path_stderr, 'w') as fe:
                    retcode = runner.write_stream_until_stop(fo, fe)
                return retcode
        else:
            def run_cmd():
                p = subp.Popen(shlex.split(cmd))
                retcode = p.wait()
                return retcode

        def func():
            if record_cmd:
                record_command()
            if change_dir:
                os.chdir(work_dir)
            retcode = run_cmd()
            if retcode > 0:
                raise subp.SubprocessError(
                    f"Command '{cmd}' run failed, return code: {retcode}")
            return retcode
        self.func = func
