import time
import typing as T
import copy
import sys

from loky.backend.process import LokyProcess

from ..jobs.process import ProcessJob
from ..condition import Condition
from ...utils import process_has_port, ProcessRunner


LauncherFunc = T.Callable[[str, int], None]


class WebAppJob(ProcessJob):
    def __init__(
            self, web_launcher: T.Union[LauncherFunc, str],
            port: int, ip: str = "127.0.0.1",
            check_times: int = 5,
            check_delta: float = 0.5,
            callback: T.Optional[T.Callable[[T.Any], None]] = None,
            error_callback: T.Optional[T.Callable[[Exception], None]] = None,
            name: T.Optional[str] = None,
            condition: T.Optional[Condition] = None,
            time_delta: float = 0.01,
            redirect_out_err: bool = False,
            **attrs
            ) -> None:
        self.ip = ip
        self.port = port
        self.check_web_launcher(web_launcher)
        self.web_launcher = web_launcher
        self.check_times = check_times
        self.check_delta = check_delta
        super().__init__(
            lambda x: x, callback=callback,
            error_callback=error_callback,
            name=name,
            condition=condition,
            time_delta=time_delta,
            redirect_out_err=redirect_out_err,
            **attrs
        )

    @staticmethod
    def check_web_launcher(web_launcher: T.Union[LauncherFunc, str]):
        if isinstance(web_launcher, str):
            cmd_temp = web_launcher
            if ('{ip}' not in cmd_temp) or ('{port}' not in cmd_temp):
                raise ValueError("web_launcher should has ip and port placeholder.")
        else:
            if not callable(web_launcher):
                raise TypeError("web_launcher should be a callable object or str.")

    def __repr__(self) -> str:
        attrs = [
            f"status={self.status}",
            f"id={self.id}",
            f"address={self.ip}:{self.port}",
        ]
        if self.condition:
            attrs.append(f" condition={self.condition}")
        attr_str = " ".join(attrs)
        return f"<{self.__class__.__name__} {attr_str}/>"

    def process_func(self):
        web_launcher = copy.copy(self.web_launcher)
        ip, port = copy.copy(self.ip), copy.copy(self.port)
        check_times = copy.copy(self.check_times)
        check_delta = copy.copy(self.check_delta)

        def check_port(pid: int) -> bool:
            for _ in range(check_times):
                time.sleep(check_delta)
                if process_has_port(pid, ip, port):
                    return True
                print(f"Process is not listen on {ip}:{port}. Try again.")
            return False

        if isinstance(web_launcher, str):
            cmd = web_launcher.format(ip=ip, port=port)
            def func():
                runner = ProcessRunner(cmd)
                runner.run()
                if check_port(runner.proc.pid):
                    retcode = runner.write_stream_until_stop(sys.stdout, sys.stderr)
                    sys.exit(retcode)
                else:
                    runner.proc.terminate()
                    raise IOError(f"Process is not listen on {ip}:{port}.")
        else:
            def func():
                proc = LokyProcess(target=web_launcher, args=(ip, port))
                proc.start()
                pid = proc.pid
                if check_port(pid):
                    proc.join()
                else:
                    proc.terminate()
                    raise IOError(f"Process is not listen on {ip}:{port}.")

        self.func = func
        super().process_func()
