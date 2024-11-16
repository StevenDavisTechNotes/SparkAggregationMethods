import logging
import queue as queue_mod
import threading
import time
from typing import Callable, Generic, TypeVar

from spark_agg_methods_common_python.utils.platform import setup_logging

logger = logging.getLogger(__name__)
TIn = TypeVar('TIn')


class Sink(Generic[TIn]):
    _actions: tuple[Callable[[TIn], None], ...]
    queue_in: queue_mod.Queue[TIn]
    _report_error: Callable[[str], None]
    _threads: list[threading.Thread]
    __slots__ = ["_actions",  "queue_in", "_report_error", "_threads"]

    def __init__(
            self,
            *,
            actions: tuple[Callable[[TIn], None], ...],
            report_error: Callable[[str], None],
            queue_in: queue_mod.Queue[TIn] | None = None
    ):
        self._actions = actions
        self._report_error = report_error

        self.queue_in = queue_in or queue_mod.Queue()
        self._threads = [
            threading.Thread(target=self._consume, args=(action,))
            for action in actions
        ]

    @property
    def num_threads(self) -> int:
        return len(self._actions)

    def _consume(
            self,
            action: Callable[[TIn], None],
    ) -> None:
        try:
            while True:
                try:
                    if self.queue_in.is_shutdown:
                        break
                    item = self.queue_in.get()
                    if self.queue_in.is_shutdown:
                        break
                    logger.debug(f"taking action on {id(item)}")
                    action(item)
                    self.queue_in.task_done()
                except queue_mod.Empty:
                    continue
                except queue_mod.ShutDown:
                    break
        except Exception as e:
            logger.exception("Error in consumer thread")
            self._report_error(str(e))

    def add_item(self, item: TIn):
        logger.debug(f"Enqueuing {id(item)}")
        try:
            if self.queue_in.is_shutdown:
                return
            self.queue_in.put(item)
        except Exception as e:
            logger.exception("Error in putting message")
            self._report_error(str(e))

    def stop(self):
        logger.debug("Stopping")
        if not self.queue_in.is_shutdown:
            self.queue_in.shutdown()
        for thread in self._threads:
            thread.join()
        logger.debug("Stopped")

    def __enter__(self):
        logger.debug("Entering")
        for thread in self._threads:
            thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        logger.debug("Exited")
        return False


if __name__ == "__main__":
    setup_logging()
    delay = 0.1
    num_threads = 5
    num_samples = 100

    def action(item: str):
        time.sleep(0.1)
        logger.info(f"Consumed {item}")
    with Sink[str](
        actions=(action,)*num_threads,
        queue_in=queue_mod.Queue(maxsize=5),
        report_error=lambda error: logger.info(f"Error: {error}"),

    ) as sink:
        start_time = time.perf_counter()
        for i in range(num_samples):
            sink.add_item(f"item{i+1}")
        sink.queue_in.join()
        end_time = time.perf_counter()
        sink.stop()
        print(f"Time taken: {end_time - start_time}")
        print(f"Expected time: {delay * num_samples / num_threads}")
