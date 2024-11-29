import contextlib
import logging
import queue as queue_mod
from typing import Callable, Generator, Generic, TypeVar

from src.utils.thread_backed_queues.pipeline_component import PipelineComponent

logger = logging.getLogger(__name__)
TOut = TypeVar('TOut')


class GeneratorSource(Generic[TOut], PipelineComponent[Callable[[], Generator[TOut, None, None]]]):
    queue_out: queue_mod.Queue[TOut]
    shutdown_is_immediate: bool

    def __init__(
            self,
            *,
            actions: tuple[Callable[[], Generator[TOut, None, None]], ...],
            block_thread_timeout: float,
            name: str,
            report_error: Callable[[str], None],
            queue_out: queue_mod.Queue[TOut] | None = None,
    ):
        self.queue_out = queue_out or queue_mod.Queue()
        self.shutdown_is_immediate = False
        super().__init__(
            actions=actions,
            block_thread_timeout=block_thread_timeout,
            name=name,
            num_threads=len(actions),
            report_error=report_error,
        )

    def _run_action_until_complete(
            self,
            action: Callable[[], Generator[TOut, None, None]],
    ) -> None:
        with contextlib.closing(action()) as generator:
            for output in generator:
                while not self.queue_out.is_shutdown:
                    try:
                        self.queue_out.put(
                            output,
                            timeout=self.block_thread_timeout,
                        )
                        break
                    except queue_mod.Full:
                        continue

    def wait_for_completion(self):
        self._wrapped_callable(
            self._wait_for_threads_to_complete
        )
        self.shutdown(
            immediate=self._keyboard_interrupted,
        )
        # self._wrapped_callable(
        #     self.queue_out.join
        # )

    def shutdown(
            self,
            immediate: bool,
    ):
        # if immediate:
        #     self.queue_out.shutdown(
        #         immediate=immediate,
        #     )
        # self._wrapped_callable(
        #     self._wait_for_threads_to_complete
        # )
        if (
            (not self.queue_out.is_shutdown)
            or (immediate != self.shutdown_is_immediate)
        ):
            self.queue_out.shutdown(
                immediate=immediate,
            )
        self.shutdown_is_immediate |= immediate
