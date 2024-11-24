import contextlib
import logging
import queue as queue_mod
import threading
import time
from typing import Callable, Generator, Generic, TypeVar

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.utils.thread_backed_queues.sink import Sink

logger = logging.getLogger(__name__)
TOut = TypeVar('TOut')


class Source(Generic[TOut]):
    _actions: tuple[Callable[[], Generator[TOut, None, None]], ...]
    queue_out: queue_mod.Queue[TOut]
    join_timeout: float
    _report_error: Callable[[str], None]
    _threads: list[threading.Thread]
    __slots__ = ["_actions", "queue_out", "join_timeout", "_report_error", "_threads"]

    def __init__(
            self,
            *,
            actions: tuple[Callable[[], Generator[TOut, None, None]], ...],
            join_timeout: float = 1.0,
            report_error: Callable[[str], None],
            queue_out: queue_mod.Queue[TOut] | None = None,
    ):
        self._actions = actions
        self.join_timeout = join_timeout
        self._report_error = report_error

        self.queue_out = queue_out or queue_mod.Queue()
        self._threads = [
            threading.Thread(target=self._process, args=(action,))
            for action in actions
        ]

    @property
    def num_threads(self) -> int:
        return len(self._actions)

    def _process(
            self,
            action: Callable[[], Generator[TOut, None, None]],
    ) -> None:
        try:
            logger.debug(f"Taking action by thread {threading.current_thread().name}")
            with contextlib.closing(action()) as generator:
                for output in generator:
                    logger.debug(f"Enqueuing output {id(output)}")
                    self.queue_out.put(output)
                    logger.debug(f"Consumed on {id(input)}")
        except queue_mod.ShutDown:
            pass
        except Exception as e:
            logger.exception("Error in consumer thread")
            self._report_error(str(e))

    def interruptable_join(self):
        for thread in self._threads:
            while thread.is_alive():
                thread.join(timeout=self.join_timeout)

    def stop(
            self,
            immediate: bool,
    ):
        logger.debug("Stopping")
        if not self.queue_out.is_shutdown:
            self.queue_out.shutdown(
                immediate=immediate,
            )
        for thread in self._threads:
            thread.join()
        logger.debug("Stopped")

    def __enter__(self):
        logger.debug("Entering")
        for thread in self._threads:
            thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop(immediate=True)
        logger.debug("Exited")
        return False


if __name__ == "__main__":
    setup_logging()
    queue_out = queue_mod.Queue(maxsize=5)
    num_sources = 2
    source_delay = 0.2
    sink_delay = 0.1
    num_messages = 10

    def make_source_starting_at(i_source: int):
        i_start = (i_source * num_messages) // num_sources
        i_stop = ((i_source+1) * num_messages) // num_sources

        def source_action() -> Generator[str, None, None]:
            for i_time in range(i_start, i_stop):
                item = str(i_time)
                logger.info(f"Sourced {item}")
                yield item
                time.sleep(source_delay)

        return source_action
    source_actions = tuple(
        make_source_starting_at(i_source) for i_source in range(num_sources))

    def sink_action(item: str) -> None:
        time.sleep(sink_delay)
        logger.info(f"Sink Consumed {item}")

    with \
            Source[str](
                actions=source_actions,
                report_error=lambda error: logger.info(f"Error: {error}"),
            ) as source, \
            Sink[str](
                actions=(sink_action,)*5,
                report_error=lambda error: logger.info(f"Error: {error}"),
            ) as sink:
        start_time = time.perf_counter()
        source.interruptable_join()
        source.stop(immediate=False)
        sink.stop(immediate=False)
        end_time = time.perf_counter()
        print(f"Time taken: {end_time - start_time}")
        expected = max(
            source_delay * num_messages / source.num_threads,
            sink_delay * num_messages / sink.num_threads)
        print(f"Expected time: {expected}")
