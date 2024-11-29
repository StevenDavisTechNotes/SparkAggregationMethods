import logging
import queue as queue_mod
from typing import Callable, Generic, TypeVar

from src.utils.thread_backed_queues.pipeline_component import PipelineComponent

logger = logging.getLogger(__name__)
TIn = TypeVar('TIn')


class Sink(Generic[TIn], PipelineComponent[Callable[[TIn], None]]):
    queue_in: queue_mod.Queue[TIn]
    shutdown_is_immediate: bool

    def __init__(
            self,
            *,
            actions: tuple[Callable[[TIn], None], ...],
            block_thread_timeout: float,
            name: str,
            queue_in: queue_mod.Queue[TIn],
            report_error: Callable[[str], None],
    ):
        self.queue_in = queue_in
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
            action: Callable[[TIn], None],
    ) -> None:
        while True:
            try:
                input = self.queue_in.get(
                    timeout=self.block_thread_timeout,
                )
            except queue_mod.Empty:
                continue
            try:
                action(input)
            finally:
                self.queue_in.task_done()

    def wait_for_completion(self):
        self._wrapped_callable(
            self.queue_in.join
        )
        self.shutdown(
            immediate=self._keyboard_interrupted,
        )

    def shutdown(
            self,
            immediate: bool,
    ):
        # if immediate:
        #     self.queue_in.shutdown(
        #         immediate=immediate,
        #     )
        # self._wrapped_callable(
        #     self._wait_for_threads_to_complete
        # )
        if (
            (not self.queue_in.is_shutdown)
            or (immediate != self.shutdown_is_immediate)
        ):
            self.queue_in.shutdown(
                immediate=immediate,
            )
        self.shutdown_is_immediate |= immediate
