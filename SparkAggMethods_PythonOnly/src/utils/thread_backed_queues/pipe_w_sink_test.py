import queue as queue_mod
import time

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.utils.thread_backed_queues.pipe import Pipe
from src.utils.thread_backed_queues.sink import Sink
from src.utils.thread_backed_queues.test_helpers import RecordingCountingSinkActionSet


def do_test_scenario(
        *,
        block_thread_timeout: float,
        processing_time_pipe: float,
        processing_time_sink: float,
        num_messages: int,
        num_threads_pipe: int,
        num_threads_sink: int,
        queue_size_pipe: int,
):
    test_sinks = RecordingCountingSinkActionSet()
    queue_1 = queue_mod.Queue()
    queue_2 = queue_mod.Queue(
        maxsize=queue_size_pipe,
    )

    def pipe_action(item: int) -> int:
        time.sleep(processing_time_pipe)
        return 2*item

    def report_error(error: str):
        assert False, f"Error: {error}"

    keyboard_interrupted: bool = False
    start_time = time.perf_counter()
    with \
            Pipe[int, int](
                actions=(pipe_action,)*num_threads_pipe,
                block_thread_timeout=block_thread_timeout,
                name="Pipe",
                queue_in=queue_1,
                queue_out=queue_2,
                report_error=report_error,
            ) as pipe, \
            Sink[int](
                actions=test_sinks.make_sink_actions(
                    num_sink_threads=num_threads_sink,
                    sink_processing_time=processing_time_sink,
                ),
                block_thread_timeout=block_thread_timeout,
                name="Sink",
                queue_in=queue_2,
                report_error=report_error,
            ) as sink:
        print(f"Started time: {time.perf_counter() - start_time}")
        try:
            for i in range(num_messages):
                queue_1.put(i)
        except KeyboardInterrupt:
            keyboard_interrupted = True
            queue_1.shutdown(immediate=True)
        except queue_mod.ShutDown:
            assert False
        print(f"Enqueued time: {time.perf_counter() - start_time}")
        pipe.wait_for_completion()
        print(f"pipe completed: {time.perf_counter() - start_time}")
        sink.wait_for_completion()
        print(f"sink completed: {time.perf_counter() - start_time}")
    print(f"Exited time: {time.perf_counter() - start_time}")
    end_time = time.perf_counter()
    keyboard_interrupted = (
        keyboard_interrupted
        or pipe.keyboard_interrupted
        or sink.keyboard_interrupted
    )
    exceptions = pipe.execution_exceptions + sink.execution_exceptions
    assert len(exceptions) == 0
    if keyboard_interrupted:
        raise KeyboardInterrupt()
    assert set(test_sinks.consumed_values) == {2*i for i in range(num_messages)}
    expected_time_per_message = (
        processing_time_pipe / pipe.num_threads,
        processing_time_sink / sink.num_threads,
    )
    expected_min = (
        4e-6*num_messages
        + (num_messages-1)*max(expected_time_per_message)
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


def test_one_sink_thread():
    do_test_scenario(
        block_thread_timeout=0.01,
        processing_time_pipe=0.1,
        processing_time_sink=0.1,
        num_threads_pipe=10,
        num_threads_sink=1,
        num_messages=100,
        queue_size_pipe=5,
    )


def test_ten_sink_threads():
    do_test_scenario(
        block_thread_timeout=0.01,
        processing_time_pipe=0.1,
        processing_time_sink=0.1,
        num_threads_pipe=10,
        num_threads_sink=10,
        num_messages=100,
        queue_size_pipe=5,
    )


def test_add_polling_time():
    do_test_scenario(
        block_thread_timeout=1.0,
        processing_time_pipe=0.1,
        processing_time_sink=0.1,
        num_threads_pipe=10,
        num_threads_sink=10,
        num_messages=100,
        queue_size_pipe=5,
    )


if __name__ == "__main__":
    setup_logging()
    try:
        # test_one_sink_thread()
        test_ten_sink_threads()
        # test_add_polling_time()
    except KeyboardInterrupt:
        print("Interrupted!")
