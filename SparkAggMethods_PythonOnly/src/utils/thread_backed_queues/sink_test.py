import queue as queue_mod
import time

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.utils.thread_backed_queues.sink import Sink
from src.utils.thread_backed_queues.test_helpers import RecordingCountingSinkActionSet


def do_test_scenario(
        *,
        block_thread_timeout: float,
        sink_processing_time: float,
        num_sink_threads: int,
        num_messages: int,
        queue_in_size: int,
):
    test_sinks = RecordingCountingSinkActionSet()
    queue_in = queue_mod.Queue(maxsize=queue_in_size)

    def report_error(error: str):
        assert False, f"Error: {error}"

    with Sink[int](
        actions=test_sinks.make_sink_actions(
            num_sink_threads=num_sink_threads,
            sink_processing_time=sink_processing_time,
        ),
        block_thread_timeout=block_thread_timeout,
        name="Sink",
        queue_in=queue_in,
        report_error=report_error,
    ) as sink:
        start_time = time.perf_counter()
        try:
            for i in range(num_messages):
                queue_in.put(i)
        except KeyboardInterrupt:
            print("Interrupted!")
            return
        except queue_mod.ShutDown:
            assert False
        print(f"Enqueued time: {time.perf_counter() - start_time}")
        sink.wait_for_completion()
        print(f"sink completed: {time.perf_counter() - start_time}")
    print(f"Exited time: {time.perf_counter() - start_time}")
    end_time = time.perf_counter()
    keyboard_interrupted = sink.keyboard_interrupted
    exceptions = sink.execution_exceptions
    assert len(exceptions) == 0
    if keyboard_interrupted:
        raise KeyboardInterrupt()
    assert set(test_sinks.consumed_values) == set(range(num_messages))
    expected_min = (
        4e-6*num_messages
        + sink_processing_time / sink.num_threads*num_messages
    )
    expected_max = 1.1*(
        expected_min
        + block_thread_timeout
    )
    elapsed_time = end_time - start_time
    print(f"Expected time: {expected_min} <= {elapsed_time} <= {expected_max}")
    assert (elapsed_time >= expected_min)
    assert (elapsed_time <= expected_max)


def test_one_sink_thread():
    do_test_scenario(
        block_thread_timeout=0.02,
        sink_processing_time=0.1,
        num_sink_threads=1,
        num_messages=10,
        queue_in_size=5,
    )


def test_ten_sink_threads():
    do_test_scenario(
        block_thread_timeout=0.02,
        sink_processing_time=0.1,
        num_sink_threads=10,
        num_messages=100,
        queue_in_size=5,
    )


def test_add_polling_time():
    do_test_scenario(
        block_thread_timeout=1.0,
        sink_processing_time=0.1,
        num_sink_threads=10,
        num_messages=100,
        queue_in_size=5,
    )


if __name__ == "__main__":
    setup_logging()
    try:
        test_one_sink_thread()
        test_ten_sink_threads()
        test_add_polling_time()
    except KeyboardInterrupt:
        print("Interrupted!")
