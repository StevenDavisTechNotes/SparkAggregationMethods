import time
from typing import Generator

from src.utils.thread_backed_queues.sync_list import ThreadSafeList


class RecordingCountingSourceActionSet():
    emitted_values: ThreadSafeList[int]

    def __init__(self):
        self.emitted_values = ThreadSafeList[int]()

    def make_source_actions(
            self,
            num_source_threads: int,
            num_messages: int,
            source_processing_time: float,
    ):
        emitted_values = self.emitted_values

        def make_source_starting_at(i_source: int):
            i_start = (i_source * num_messages) // num_source_threads
            i_stop = ((i_source+1) * num_messages) // num_source_threads

            def source_action() -> Generator[int, None, None]:
                for i_time in range(i_start, i_stop):
                    item = i_time
                    yield item
                    emitted_values.append(item)
                    time.sleep(source_processing_time)

            return source_action

        source_actions = tuple(
            make_source_starting_at(i_source)
            for i_source in range(num_source_threads)
        )
        return source_actions


class RecordingCountingSinkActionSet():
    consumed_values: ThreadSafeList[int]

    def __init__(self):
        self.consumed_values = ThreadSafeList[int]()

    def make_sink_actions(
            self,
            num_sink_threads: int,
            sink_processing_time: float,
    ):
        consumed_values = self.consumed_values

        def sink_action(item: int) -> None:
            consumed_values.append(item)
            time.sleep(sink_processing_time)

        source_actions = (sink_action,)*num_sink_threads
        return source_actions
