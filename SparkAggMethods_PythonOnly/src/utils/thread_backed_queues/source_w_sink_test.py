import queue as queue_mod
import time

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.utils.thread_backed_queues.sink import Sink
from src.utils.thread_backed_queues.source import GeneratorSource
from src.utils.thread_backed_queues.test_helpers import RecordingCountingSinkActionSet, RecordingCountingSourceActionSet


def do_test_scenario(
        *,
        block_thread_timeout: float,
        processing_time_source: float,
        processing_time_sink: float,
        num_threads_source: int,
        num_threads_sink: int,
        num_messages: int,
        queue_in_size: int,
):
    test_generators = RecordingCountingSourceActionSet()
    test_sinks = RecordingCountingSinkActionSet()

    def report_error(error: str):
        assert False, f"Error: {error}"

    start_time = time.perf_counter()
    with \
            GeneratorSource[int](
                actions=test_generators.make_source_actions(
                    num_source_threads=num_threads_source,
                    num_messages=num_messages,
                    source_processing_time=processing_time_source,
                ),
                block_thread_timeout=block_thread_timeout,
                name="Source",
                queue_out=queue_mod.Queue(maxsize=queue_in_size),
                report_error=report_error,
            ) as source, \
            Sink[int](
                actions=test_sinks.make_sink_actions(
                    num_sink_threads=num_threads_sink,
                    sink_processing_time=processing_time_sink,
                ),
                block_thread_timeout=block_thread_timeout,
                name="Sink",
                queue_in=source.queue_out,
                report_error=report_error,
            ) as sink:
        print(f"Started time: {time.perf_counter() - start_time}")
        source.wait_for_completion()
        print(f"source completed: {time.perf_counter() - start_time}")
        sink.wait_for_completion()
        print(f"sink completed: {time.perf_counter() - start_time}")
    print(f"exited completed: {time.perf_counter() - start_time}")
    end_time = time.perf_counter()
    keyboard_interrupted = (
        source.keyboard_interrupted
        or sink.keyboard_interrupted
    )
    exceptions = source.execution_exceptions + sink.execution_exceptions
    assert len(exceptions) == 0
    if keyboard_interrupted:
        raise KeyboardInterrupt()
    assert set(test_generators.emitted_values) == set(range(num_messages))
    assert set(test_sinks.consumed_values) == set(range(num_messages))
    expected_time_per_message = (
        processing_time_source / source.num_threads,
        processing_time_sink / sink.num_threads,
    )
    expected_min = (
        (num_messages-1)*max(expected_time_per_message)
        + min(expected_time_per_message)
    )
    expected_max = 1.1*(
        expected_min
        - min(expected_time_per_message)
        + sum(expected_time_per_message)
        + block_thread_timeout
    )
    elapsed_time = end_time - start_time
    print(f"Time taken: {elapsed_time}")
    print(f"Expected time: {expected_min} <= {elapsed_time} <= {expected_max}")
    assert (elapsed_time >= expected_min)
    assert (elapsed_time <= expected_max)


def test_single_threaded():
    do_test_scenario(
        block_thread_timeout=0.001,
        processing_time_source=0.02,
        processing_time_sink=0.1,
        num_threads_source=1,
        num_threads_sink=1,
        num_messages=10,
        queue_in_size=5,
    )


def test_one_sink_thread():
    do_test_scenario(
        block_thread_timeout=0.0,
        processing_time_source=0.02,
        processing_time_sink=0.1,
        num_threads_source=2,
        num_threads_sink=1,
        num_messages=10,
        queue_in_size=5,
    )


def test_ten_sink_threads():
    do_test_scenario(
        block_thread_timeout=0.1,
        processing_time_source=0.02,
        processing_time_sink=0.1,
        num_threads_source=2,
        num_threads_sink=10,
        num_messages=100,
        queue_in_size=5,
    )


def test_add_polling_time():
    do_test_scenario(
        block_thread_timeout=1.0,
        processing_time_source=0.02,
        processing_time_sink=0.1,
        num_threads_source=2,
        num_threads_sink=10,
        num_messages=100,
        queue_in_size=5,
    )


if __name__ == "__main__":
    setup_logging()
    try:
        test_single_threaded()
        test_one_sink_thread()
        test_ten_sink_threads()
        test_add_polling_time()
    except KeyboardInterrupt:
        print("Interrupted!")
