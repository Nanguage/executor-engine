import time
import typing as T
import copy
import sys

from loky.backend.process import LokyProcess
from cmd2func.runner import ProcessRunner

from ..process import ProcessJob
from ..base import Job
from ..condition import Condition
from ...utils import PortManager, ExecutorError


LauncherFunc = T.Callable[[str, int], None]


def WebAppJob(
    web_launcher: T.Union[LauncherFunc, str],
    ip: str = "127.0.0.1", port: T.Optional[int] = None,
    base_class: T.Type[Job] = ProcessJob,
    check_times: int = 6,
    check_delta: float = 1.0,
    callback: T.Optional[T.Callable[[T.Any], None]] = None,
    error_callback: T.Optional[T.Callable[[Exception], None]] = None,
    retries: int = 0,
    retry_time_delta: float = 0.0,
    name: T.Optional[str] = None,
    condition: T.Optional[Condition] = None,
    time_delta: float = 0.01,
    redirect_out_err: bool = False,
    **attrs
):
    class _WebAppJob(base_class):
        def __init__(self) -> None:
            self.ip = ip
            if ip not in ("127.0.0.1", "localhost", "0.0.0.0"):
                raise NotImplementedError(
                    "WebAppJob now only support launch in local mechine.")
            self.port = port
            self.check_web_launcher(web_launcher)
            self.web_launcher = web_launcher
            self.check_times = check_times
            self.check_delta = check_delta
            if isinstance(web_launcher, str):
                attrs.update({"cmd": web_launcher})
            super().__init__(
                lambda x: x, callback=callback,
                error_callback=error_callback,
                retries=retries,
                retry_time_delta=retry_time_delta,
                name=name,
                condition=condition,
                wait_time_delta=time_delta,
                redirect_out_err=redirect_out_err,
                **attrs
            )

        @staticmethod
        def check_web_launcher(web_launcher: T.Union[LauncherFunc, str]):
            if isinstance(web_launcher, str):
                cmd_temp = web_launcher
                if ('{ip}' not in cmd_temp) or ('{port}' not in cmd_temp):
                    raise ValueError(
                        "web_launcher should has ip and port placeholder.")
            else:
                if not callable(web_launcher):
                    raise TypeError(
                        "web_launcher should be a callable object or str.")

        def __repr__(self) -> str:
            attrs = [
                f"status={self.status}",
                f"id={self.id}",
                f"address={self.ip}:{self.port}",
            ]
            if self.condition:
                attrs.append(f" condition={repr(self.condition)}")
            attr_str = " ".join(attrs)
            return f"<{self.__class__.__name__} {attr_str}/>"

        def consume_resource(self) -> bool:
            if super().consume_resource():
                if self.port is None:
                    self.port = PortManager.get_port()
                    self.attrs.update({"address": f"{self.ip}:{self.port}"})
                else:
                    PortManager.consume_port(self.port)
                return True
            else:  # pragma: no cover
                return False

        def release_resource(self) -> bool:
            if self.port is None:  # pragma: no cover
                return False
            if super().release_resource():
                PortManager.release_port(self.port)
                return True
            else:  # pragma: no cover
                return False

        def process_func(self):
            web_launcher = copy.copy(self.web_launcher)
            if self.port is None:  # pragma: no cover
                raise ExecutorError("Unreachable code.")
            ip, port = copy.copy(self.ip), copy.copy(self.port)
            check_times = copy.copy(self.check_times)
            check_delta = copy.copy(self.check_delta)

            def check_port(pid: int) -> bool:  # pragma: no cover
                for _ in range(check_times):
                    time.sleep(check_delta)
                    if PortManager.process_has_port(pid, ip, port):
                        return True
                    print(f"Process is not listen on {ip}:{port}. Try again.")
                return False

            if isinstance(web_launcher, str):
                cmd = web_launcher.format(ip=ip, port=port)

                def func():  # pragma: no cover
                    runner = ProcessRunner(cmd)
                    runner.run()
                    if check_port(runner.proc.pid):
                        retcode = runner.write_stream_until_stop(
                            sys.stdout, sys.stderr)
                        sys.exit(retcode)
                    else:
                        runner.proc.terminate()
                        raise IOError(f"Process is not listen on {ip}:{port}.")
            else:
                def func():  # pragma: no cover
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
    return _WebAppJob()
