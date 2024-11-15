import logging
import queue as queue_mod
import threading
import time
from typing import Callable, Generic, TypeVar

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.utils.thread_backed_queues.sink import Sink

logger = logging.getLogger(__name__)
TIn = TypeVar('TIn')
TOut = TypeVar('TOut')


class Pipe(Generic[TIn, TOut]):
    _action: Callable[[TIn], TOut]
    num_threads: int
    queue_in: queue_mod.Queue[TIn]
    queue_out: queue_mod.Queue[TOut]
    _report_error: Callable[[str], None]
    _threads: list[threading.Thread]

    def __init__(
            self,
            *,
            action: Callable[[TIn], TOut],
            num_threads: int,
            report_error: Callable[[str], None],
            queue_in: queue_mod.Queue[TIn] | None = None,
            queue_out: queue_mod.Queue[TOut] | None = None,
    ):
        self._action = action
        self.num_threads = num_threads
        self._report_error = report_error

        self.queue_in = queue_in or queue_mod.Queue()
        self.queue_out = queue_out or queue_mod.Queue()
        self._threads = [
            threading.Thread(target=self._consume)
            for _ in range(num_threads)
        ]

    def _consume(self):
        try:
            while True:
                if self.queue_in.is_shutdown or self.queue_out.is_shutdown:
                    break
                input = self.queue_in.get()
                if self.queue_in.is_shutdown or self.queue_out.is_shutdown:
                    break
                logger.debug(f"Taking action on {id(input)}")
                output = self._action(input)
                logger.debug(f"Enqueuing output {id(output)}")
                self.queue_out.put(output)
                logger.debug(f"Consumed on {id(input)}")
                self.queue_in.task_done()
        except queue_mod.ShutDown:
            pass
        except Exception as e:
            logger.exception("Error in consumer thread")
            self._report_error(str(e))

    def stop(self):
        logger.debug("Stopping")
        if not self.queue_in.is_shutdown:
            self.queue_in.shutdown()
        if not self.queue_out.is_shutdown:
            self.queue_out.shutdown()
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
    queue_in = queue_mod.Queue()
    queue_out = queue_mod.Queue(maxsize=5)
    timeout = 0.1
    num_messages = 100

    def pipe_action(item: str) -> str:
        time.sleep(timeout)
        logger.info(f"Pipe Consumed {item}")
        return item

    def sink_action(item: str) -> None:
        time.sleep(timeout)
        logger.info(f"Sink Consumed {item}")

    with \
            Pipe[str, str](
                action=pipe_action,
                num_threads=5,
                queue_in=queue_in,
                queue_out=queue_out,
                report_error=lambda error: logger.info(f"Error: {error}"),
            ) as pipe, \
            Sink[str](
                action=sink_action,
                num_threads=5,
                queue_in=queue_out,
                report_error=lambda error: logger.info(f"Error: {error}"),
            ) as sink:
        start_time = time.perf_counter()
        for i in range(1, num_messages):
            queue_in.put(f"item{i}")
        queue_in.join()
        queue_out.join()
        end_time = time.perf_counter()
        pipe.stop()
        sink.stop()
        print(f"Time taken: {end_time - start_time}")
        expected = timeout * num_messages / min(pipe.num_threads, sink.num_threads)
        print(f"Expected time: {expected}")
