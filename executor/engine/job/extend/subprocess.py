import asyncio
import subprocess as subp
import shlex
import copy

from ..jobs.process import ProcessJob
from ...utils import ProcessRunner


class SubprocessJob(ProcessJob):
    def __init__(self, cmd: str, **kwargs):
        self.cmd = cmd
        super().__init__(lambda x: x, **kwargs)

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

    async def wait_and_run(self):
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
                                fe.write(line.rsplit("\n"))
                        except StopIteration as e:
                            retcode = e.value
                            break
                return retcode
        else:
            def func():
                p = subp.Popen(shlex.split(cmd))
                return p.wait()
        self.func = func
        while True:
            if self.runnable() and self.consume_resource():
                self.status = "running"
                res = await self.run()
                return res
            else:
                await asyncio.sleep(self.time_delta)
