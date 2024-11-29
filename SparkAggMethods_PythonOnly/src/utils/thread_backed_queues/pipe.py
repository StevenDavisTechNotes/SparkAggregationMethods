import logging
import queue as queue_mod
from typing import Callable, Generic, TypeVar

from src.utils.thread_backed_queues.pipeline_component import PipelineComponent

logger = logging.getLogger(__name__)
TIn = TypeVar('TIn')
TOut = TypeVar('TOut')


class Pipe(Generic[TIn, TOut], PipelineComponent[Callable[[TIn], TOut]]):
    queue_in: queue_mod.Queue[TIn]
    queue_out: queue_mod.Queue[TOut]
    shutdown_is_immediate: bool

    def __init__(
            self,
            *,
            actions: tuple[Callable[[TIn], TOut], ...],
            block_thread_timeout: float,
            name: str,
            queue_in: queue_mod.Queue[TIn],
            queue_out: queue_mod.Queue[TOut] | None = None,
            report_error: Callable[[str], None],
    ):
        self.queue_in = queue_in
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
            action: Callable[[TIn], TOut],
    ) -> None:
        while not self.queue_out.is_shutdown:
            try:
                input = self.queue_in.get(
                    timeout=self.block_thread_timeout,
                )
            except queue_mod.Empty:
                continue
            try:
                if self.queue_out.is_shutdown:
                    break
                output = action(input)
                self.queue_out.put(output)
            finally:
                self.queue_in.task_done()

    def wait_for_completion(self):
        self._wrapped_callable(
            self.queue_in.join
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
        if (
            (not self.queue_out.is_shutdown)
            or (immediate != self.shutdown_is_immediate)
        ):
            self.queue_out.shutdown(
                immediate=immediate,
            )
        self.shutdown_is_immediate |= immediate
