import threading


class ExceptionRecordingThread(threading.Thread):
    _exception: Exception | None
    _keyboard_interrupted: bool

    def __init__(self, *args, **kwargs):
        self._exception = None
        self._keyboard_interrupted = False
        super().__init__(*args, **kwargs)

    def run(self):
        try:
            super().run()
        except Exception as e:
            self._exception = e

    @property
    def keyboard_interrupted(self) -> bool:
        return self._keyboard_interrupted

    @property
    def stopped_on_exception(self):
        return self._exception
