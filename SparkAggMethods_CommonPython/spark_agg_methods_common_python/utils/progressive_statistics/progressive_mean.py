import math

import numpy


class ProgressiveMean:
    count_so_far: int = 0
    mean_so_far: float = 0.0

    def update(self, batch: numpy.ndarray):
        if len(batch) == 0:
            return
        if self.count_so_far == 0:
            self.mean_so_far = batch.mean()
            self.count_so_far = len(batch)
            return
        n_a = self.count_so_far
        n_b = len(batch)
        n_ab = n_a + n_b
        mean_a = self.mean_so_far
        mean_b = batch.mean()
        delta = mean_b - mean_a
        self.mean_so_far += delta * n_b / n_ab
        self.count_so_far = n_ab

    @property
    def mean(self) -> float:
        if self.count_so_far == 0:
            return math.nan
        return self.mean_so_far
