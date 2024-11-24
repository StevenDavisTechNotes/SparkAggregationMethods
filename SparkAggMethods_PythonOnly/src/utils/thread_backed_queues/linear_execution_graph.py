# usage: python -m src.utils.thread_backed_queues.linear_execution_graph

import logging
import queue as queue_mod
import time
from typing import Callable, Generator, TypeVar

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.utils.thread_backed_queues import Pipe, Sink, Source

logger = logging.getLogger(__name__)
T0 = TypeVar('T0')
T1 = TypeVar('T1')
T2 = TypeVar('T2')


def execute_in_three_stages(
        *,
        actions_0: tuple[Callable[[], Generator[T0, None, None]], ...],
        actions_1: tuple[Callable[[T0], T1], ...],
        actions_2: tuple[Callable[[T1], T2], ...],
        actions_3: tuple[Callable[[T2], None], ...],
        queue_0: queue_mod.Queue[T0] | None = None,
        queue_1: queue_mod.Queue[T1] | None = None,
        queue_2: queue_mod.Queue[T2] | None = None,
        report_error: Callable[[str], None],
) -> bool:
    queue_0 = queue_0 or queue_mod.Queue()
    queue_1 = queue_1 or queue_mod.Queue()
    queue_2 = queue_2 or queue_mod.Queue()

    def stop(
            immediate: bool,
    ):
        logger.debug("Stopping")
        queue_0.shutdown(immediate=immediate)
        queue_1.shutdown(immediate=immediate)
        queue_2.shutdown(immediate=immediate)
        logger.debug("Stopped")

    def wrapped_report_error(error: str) -> None:
        report_error(error)
        stop(immediate=True)

    success: bool = False
    with \
            Source[T0](
                actions=actions_0,
                queue_out=queue_0,
                report_error=wrapped_report_error,
            ) as source, \
            Pipe[T0, T1](
                actions=actions_1,
                queue_in=queue_0,
                queue_out=queue_1,
                report_error=wrapped_report_error,
            ), \
            Pipe[T1, T2](
                actions=actions_2,
                queue_in=queue_1,
                queue_out=queue_2,
                report_error=wrapped_report_error,
            ), \
            Sink[T2](
                actions=actions_3,
                queue_in=queue_2,
                report_error=wrapped_report_error,
            ):
        try:
            source.interruptable_join()
            queue_0.join()
            queue_1.join()
            queue_2.join()
            success = True
        except KeyboardInterrupt:
            stop(immediate=True)
        except Exception as e:
            wrapped_report_error(str(e))
            stop(immediate=True)
    return success


if __name__ == "__main__":
    setup_logging()
    queue_0 = queue_mod.Queue(maxsize=5)
    queue_1 = queue_mod.Queue(maxsize=5)
    queue_2 = queue_mod.Queue(maxsize=5)
    delay = 0.1
    num_messages = 100
    num_threads_1 = 5

    def source_action() -> Generator[str, None, None]:
        for i in range(num_messages):
            yield f"item{i+1}"

    def pipe_1_action(item: str) -> str:
        time.sleep(delay)
        logger.info(f"Pipe-1 {item}")
        return item

    def pipe_2_action(item: str) -> str:
        logger.info(f"Pipe-2 {item}")
        return item

    def sink_action(item: str) -> None:
        logger.info(f"Sink {item}")

    start_time = time.perf_counter()
    if execute_in_three_stages(
        actions_0=(source_action,),
        actions_1=(pipe_1_action,)*num_threads_1,
        actions_2=(pipe_2_action,),
        actions_3=(sink_action,),
        queue_0=queue_0,
        queue_1=queue_1,
        queue_2=queue_2,
        report_error=lambda error: logger.info(f"Error: {error}"),
    ):
        logger.info("Success")
        end_time = time.perf_counter()
        print(f"Time taken: {end_time - start_time}")
        expected = delay * num_messages / num_threads_1
        print(f"Expected time: {expected}")
    else:
        logger.error("Failed")
