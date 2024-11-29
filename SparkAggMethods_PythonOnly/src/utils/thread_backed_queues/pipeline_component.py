import logging
import queue as queue_mod
from abc import ABC, abstractmethod
from typing import Callable, Generic, TypeVar

from src.utils.thread_backed_queues.error_propagating_thread import ExceptionRecordingThread
from src.utils.thread_backed_queues.sync_list import ThreadSafeList

logger = logging.getLogger(__name__)
TAction = TypeVar('TAction', bound=Callable)


class PipelineComponent(Generic[TAction], ABC):
    _actions: tuple[TAction, ...]
    _execution_exceptions: ThreadSafeList[Exception]
    block_thread_timeout: float
    _keyboard_interrupted: bool
    _num_threads: int
    name: str
    _report_error: Callable[[str], None]
    _threads: tuple[ExceptionRecordingThread, ...]

    def __init__(
            self,
            *,
            actions: tuple[TAction, ...],
            block_thread_timeout: float,
            name: str,
            num_threads: int,
            report_error: Callable[[str], None],
    ):
        self._actions = actions
        self._execution_exceptions = ThreadSafeList[Exception]()
        self.block_thread_timeout = block_thread_timeout
        self._keyboard_interrupted = False
        self.name = name
        self._num_threads = num_threads
        self._report_error = report_error

        self._threads = tuple(
            ExceptionRecordingThread(
                target=self._wrapped_run_action,
                args=(action,),
            )
            for action in actions
        )

    @abstractmethod
    def _run_action_until_complete(
            self,
            action: TAction,
    ) -> None:
        ...

    def _wrapped_run_action(
            self,
            action: TAction,
    ) -> None:
        try:
            self._run_action_until_complete(action)
        except queue_mod.ShutDown:
            pass
        except KeyboardInterrupt:
            self._keyboard_interrupted = True
        except Exception as e:
            self._execution_exceptions.append(e)
            self._report_error(str(e))

    def _wrapped_callable(
            self,
            action: Callable[[], None],
    ) -> None:
        try:
            action()
        except queue_mod.ShutDown:
            pass
        except KeyboardInterrupt:
            self._keyboard_interrupted = True
        except Exception as e:
            self._execution_exceptions.append(e)
            self._report_error(str(e))

    def _wait_for_threads_to_complete(
            self,
    ) -> None:
        possibly_live_threads: list[ExceptionRecordingThread] = list(self._threads)
        while len(possibly_live_threads) > 0:
            still_live_threads: list[ExceptionRecordingThread] = []
            for thread in possibly_live_threads:
                thread.join(
                    timeout=self.block_thread_timeout,
                )
                if thread.is_alive():
                    still_live_threads.append(thread)
            # if len(still_live_threads) > 0:
                # logger.debug(f"Waiting for {len(still_live_threads)} threads to complete")
            possibly_live_threads = still_live_threads

    @abstractmethod
    def shutdown(
            self,
            immediate: bool,
    ):
        ...

    @property
    def num_threads(self) -> int:
        return len(self._actions)

    @property
    def keyboard_interrupted(self) -> bool:
        return (
            self._keyboard_interrupted
            or any(thread.keyboard_interrupted for thread in self._threads)
        )

    @property
    def execution_exceptions(self) -> list[Exception]:
        collected: list[Exception] = list()
        collected.extend(self._execution_exceptions)
        for thread in self._threads:
            if thread.stopped_on_exception is not None:
                collected.append(thread.stopped_on_exception)
        return collected

    def __enter__(self):
        for thread in self._threads:
            thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(immediate=False)
        self._wrapped_callable(
            self._wait_for_threads_to_complete
        )
