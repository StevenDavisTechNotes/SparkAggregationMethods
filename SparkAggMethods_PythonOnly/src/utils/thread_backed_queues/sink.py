import logging
import queue as queue_mod
import threading
import time
from typing import Callable, Generic, TypeVar

from spark_agg_methods_common_python.utils.platform import setup_logging

logger = logging.getLogger(__name__)
TIn = TypeVar('TIn')


class Sink(Generic[TIn]):
    _action: Callable[[TIn], None]
    num_threads: int
    polling_timeout: float
    queue_in: queue_mod.Queue[TIn]
    _report_error: Callable[[str], None]
    _threads: list[threading.Thread]
    __slots__ = ["_action", "num_threads", "polling_timeout", "queue_in", "_report_error", "_threads"]

    def __init__(
            self,
            *,
            action: Callable[[TIn], None],
            num_threads: int,
            report_error: Callable[[str], None],
            queue_in: queue_mod.Queue[TIn] | None = None
    ):
        self._action = action
        self.num_threads = num_threads
        self._report_error = report_error

        self.queue_in = queue_in or queue_mod.Queue()
        self._threads = [
            threading.Thread(target=self._consume)
            for _ in range(num_threads)
        ]

    def _consume(self):
        try:
            while True:
                try:
                    if self.queue_in.is_shutdown:
                        break
                    item = self.queue_in.get()
                    if self.queue_in.is_shutdown:
                        break
                    logger.debug(f"taking action on {id(item)}")
                    self._action(item)
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

    def action(item: str):
        time.sleep(0.2)
        logger.info(f"Consumed {item}")
    with Sink[str](
        action=action,
        num_threads=5,
        queue_in=queue_mod.Queue(maxsize=5),
        report_error=lambda error: logger.info(f"Error: {error}"),
    ) as sink:
        start_time = time.perf_counter()
        sink.queue_in.put("item1")
        sink.queue_in.put("item2")
        sink.add_item("item3")
        sink.add_item("item4")
        for i in range(5, 100):
            sink.add_item(f"item{i}")
        sink.queue_in.join()
        end_time = time.perf_counter()
        sink.stop()
        print(f"Time taken: {end_time - start_time}")
        print(f"Expected time: {0.2 * 100 / sink.num_threads}")
