import queue as queue_mod
import time

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.utils.thread_backed_queues.linear_execution_graph import execute_in_three_stages
from src.utils.thread_backed_queues.test_helpers import RecordingCountingSinkActionSet, RecordingCountingSourceActionSet


def do_test_scenario(
        *,
        block_thread_timeout: float,
        processing_time_source: float,
        processing_time_pipe_1: float,
        processing_time_pipe_2: float,
        processing_time_sink: float,
        num_messages: int,
        num_threads_source: int,
        num_threads_pipe_1: int,
        num_threads_pipe_2: int,
        num_threads_sink: int,
        queue_size_source: int,
        queue_size_pipe_1: int,
        queue_size_pipe_2: int,
):
    test_generators = RecordingCountingSourceActionSet()
    test_sinks = RecordingCountingSinkActionSet()
    queue_0 = queue_mod.Queue(maxsize=queue_size_source)
    queue_1 = queue_mod.Queue(maxsize=queue_size_pipe_1)
    queue_2 = queue_mod.Queue(maxsize=queue_size_pipe_2)
    prime_1 = 2
    prime_2 = 3

    def pipe_1_action(item: int) -> int:
        time.sleep(processing_time_pipe_1)
        return prime_1*item

    def pipe_2_action(item: int) -> int:
        time.sleep(processing_time_pipe_2)
        return prime_2*item

    def report_error(error: str):
        assert False, f"Error: {error}"

    start_time = time.perf_counter()
    execute_in_three_stages(
        actions_0=test_generators.make_source_actions(
            num_source_threads=num_threads_source,
            num_messages=num_messages,
            source_processing_time=processing_time_source,
        ),
        actions_1=(pipe_1_action,)*num_threads_pipe_1,
        actions_2=(pipe_2_action,)*num_threads_pipe_2,
        actions_3=test_sinks.make_sink_actions(
            num_sink_threads=num_threads_sink,
            sink_processing_time=processing_time_sink,
        ),
        queue_0=queue_0,
        queue_1=queue_1,
        queue_2=queue_2,
        report_error=report_error,
    )
    end_time = time.perf_counter()
    assert set(test_generators.emitted_values) == set(range(num_messages))
    assert set(test_sinks.consumed_values) == {
        prime_1*prime_2*i for i in range(num_messages)}
    expected_time_per_message = (
        processing_time_source / num_threads_source,
        processing_time_pipe_1 / num_threads_pipe_1,
        processing_time_pipe_2 / num_threads_pipe_2,
        processing_time_sink / num_threads_sink,
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
    print(f"Expected time: {expected_min} <= {elapsed_time} <= {expected_max}")
    assert (elapsed_time >= expected_min)
    assert (elapsed_time <= expected_max)


def test_single_threaded_sink_bottleneck():
    do_test_scenario(
        block_thread_timeout=0.1,
        processing_time_source=0.01,
        processing_time_pipe_1=0.01,
        processing_time_pipe_2=0.01,
        processing_time_sink=0.1,
        num_threads_source=1,
        num_threads_pipe_1=1,
        num_threads_pipe_2=1,
        num_threads_sink=1,
        num_messages=10,
        queue_size_source=5,
        queue_size_pipe_1=5,
        queue_size_pipe_2=5,
    )


def test_dual_threaded_sink_bottleneck():
    do_test_scenario(
        block_thread_timeout=0.1,
        processing_time_source=0.01,
        processing_time_pipe_1=0.01,
        processing_time_pipe_2=0.01,
        processing_time_sink=0.1,
        num_threads_source=2,
        num_threads_pipe_1=2,
        num_threads_pipe_2=2,
        num_threads_sink=10,
        num_messages=100,
        queue_size_source=-1,
        queue_size_pipe_1=-1,
        queue_size_pipe_2=-1,
    )


def test_dual_threaded_pipe_bottleneck():
    do_test_scenario(
        block_thread_timeout=0.1,
        processing_time_source=0.01,
        processing_time_pipe_1=0.1,
        processing_time_pipe_2=0.1,
        processing_time_sink=0.01,
        num_threads_source=2,
        num_threads_pipe_1=2,
        num_threads_pipe_2=2,
        num_threads_sink=10,
        num_messages=100,
        queue_size_source=-1,
        queue_size_pipe_1=-1,
        queue_size_pipe_2=-1,
    )


if __name__ == "__main__":
    setup_logging()
    try:
        test_single_threaded_sink_bottleneck()
        test_dual_threaded_sink_bottleneck()
        test_dual_threaded_pipe_bottleneck()
    except KeyboardInterrupt:
        print("Interrupted!")
