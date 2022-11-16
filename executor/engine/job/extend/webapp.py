import time
import typing as T
import copy
import psutil

from loky.backend.process import LokyProcess

from ..jobs.process import ProcessJob
from ..condition import Condition


class WebAppJob(ProcessJob):
    def __init__(
            self, web_launcher: T.Callable[[str, int], None],
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

        def check_address(target_pid: int) -> bool:
            p = psutil.Process(target_pid)
            addrs = [
                (c.laddr.ip, c.laddr.port) for c in p.connections()
            ]
            return (ip, port) in addrs

        def func():
            proc = LokyProcess(target=web_launcher, args=(ip, port))
            proc.start()
            pid = proc.pid
            for _ in range(check_times):
                time.sleep(check_delta)
                if check_address(pid):
                    break
                print(f"Process is not listen on {ip}:{port}. Try again.")
            else:
                proc.terminate()
                raise IOError(f"Process is not listen on {ip}:{port}.")
            proc.join()

        self.func = func
        super().process_func()
