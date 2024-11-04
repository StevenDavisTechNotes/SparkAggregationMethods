import math

import numpy


class ProgressiveMax:
    max_so_far: float | None = None

    def update(self, batch: numpy.ndarray):
        if len(batch) == 0:
            return
        new_max = batch.max()
        if self.max_so_far is None or new_max > self.max_so_far:
            self.max_so_far = new_max

    @property
    def max(self) -> float:
        return self.max_so_far or math.nan
