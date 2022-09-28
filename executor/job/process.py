from .base import Job


class ProcessJob(Job):
    def has_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            return self.engine.process_count > 0

    def consume_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.process_count -= 1
            return True

    def release_resource(self) -> bool:
        if self.engine is None:
            return False
        else:
            self.engine.process_count += 1
            return True

    def run(self):
        from pathos.multiprocessing import Pool
        self._pool = Pool(processes=1)
        self._future = self._pool.apply_async(
            self.func, tuple(self.args),
            callback=self.on_done,
            error_callback=self.on_failed,
        )

    def cancel_task(self):
        self._pool.terminate()
        del self._pool
