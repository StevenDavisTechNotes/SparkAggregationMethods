import logging
import queue as queue_mod
import threading
import time
from typing import Callable, Generic, TypeVar

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.utils.thread_backed_queues.pipe import Pipe
from src.utils.thread_backed_queues.sink import Sink

logger = logging.getLogger(__name__)
T0 = TypeVar('T0')
T1 = TypeVar('T1')
T2 = TypeVar('T2')


class ThreeStepExecutionGraph(Generic[T0, T1, T2]):
    action_0: Callable[[queue_mod.Queue[T0]], None]
    action_1: Callable[[T0], T1]
    action_2: Callable[[T1], T2]
    action_3: Callable[[T2], None]
    num_threads_1: int
    num_threads_2: int
    num_threads_3: int
    queue_0: queue_mod.Queue[T0]
    queue_1: queue_mod.Queue[T1]
    queue_2: queue_mod.Queue[T2]
    report_error: Callable[[str], None]

    def __init__(
            self,
            *,
            action_0: Callable[[queue_mod.Queue[T0]], None],
            action_1: Callable[[T0], T1],
            action_2: Callable[[T1], T2],
            action_3: Callable[[T2], None],
            num_threads_1: int,
            num_threads_2: int,
            num_threads_3: int,
            queue_0: queue_mod.Queue[T0] | None = None,
            queue_1: queue_mod.Queue[T1] | None = None,
            queue_2: queue_mod.Queue[T2] | None = None,
            report_error: Callable[[str], None],
    ):
        self.action_0 = action_0
        self.action_1 = action_1
        self.action_2 = action_2
        self.action_3 = action_3
        self.num_threads_1 = num_threads_1
        self.num_threads_2 = num_threads_2
        self.num_threads_3 = num_threads_3
        self.queue_0 = queue_0 or queue_mod.Queue()
        self.queue_1 = queue_1 or queue_mod.Queue()
        self.queue_2 = queue_2 or queue_mod.Queue()
        self.report_error = report_error

    def run(self):
        with \
                Pipe[T0, T1](
                    action=self.action_1,
                    num_threads=self.num_threads_1,
                    queue_in=self.queue_0,
                    queue_out=self.queue_1,
                    report_error=self.report_error,
                ) as pipe_1, \
                Pipe[T1, T2](
                    action=self.action_2,
                    num_threads=self.num_threads_2,
                    queue_in=self.queue_1,
                    queue_out=self.queue_2,
                    report_error=self.report_error,
                ) as pipe_2, \
                Sink[T2](
                    action=self.action_3,
                    num_threads=self.num_threads_3,
                    queue_in=self.queue_2,
                    report_error=self.report_error,
                ) as sink:
            try:
                action_0_thread = threading.Thread(target=self.action_0, args=(self.queue_0,))
                action_0_thread.start()
                action_0_thread.join()
                self.queue_0.join()
                self.queue_1.join()
                self.queue_2.join()
            finally:
                pipe_1.stop()
                pipe_2.stop()
                sink.stop()

    def stop(self):
        logger.debug("Stopping")
        self.queue_0.shutdown()
        self.queue_1.shutdown()
        self.queue_2.shutdown()
        logger.debug("Stopped")


if __name__ == "__main__":
    setup_logging()
    queue_0 = queue_mod.Queue(maxsize=5)
    queue_1 = queue_mod.Queue(maxsize=5)
    queue_2 = queue_mod.Queue(maxsize=5)
    timeout = 0.1
    num_messages = 100
    num_threads_1 = 5

    def source_action(queue_in: queue_mod.Queue) -> None:
        for i in range(1, num_messages):
            queue_in.put(f"item{i}")

    def pipe_1_action(item: str) -> str:
        time.sleep(timeout)
        logger.info(f"Pipe-1 {item}")
        return item

    def pipe_2_action(item: str) -> str:
        logger.info(f"Pipe-2 {item}")
        return item

    def sink_action(item: str) -> None:
        logger.info(f"Sink {item}")

    network = ThreeStepExecutionGraph[str, str, str](
        action_0=source_action,
        action_1=pipe_1_action,
        action_2=pipe_2_action,
        action_3=sink_action,
        num_threads_1=num_threads_1,
        num_threads_2=1,
        num_threads_3=1,
        queue_0=queue_0,
        queue_1=queue_1,
        queue_2=queue_2,
        report_error=lambda error: logger.info(f"Error: {error}"),
    )
    start_time = time.perf_counter()
    network.run()
    end_time = time.perf_counter()
    print(f"Time taken: {end_time - start_time}")
    expected = timeout * num_messages / num_threads_1
    print(f"Expected time: {expected}")
