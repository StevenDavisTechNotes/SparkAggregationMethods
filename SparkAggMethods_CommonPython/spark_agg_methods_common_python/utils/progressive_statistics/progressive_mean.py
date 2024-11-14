import math

import pandas as pd


class ProgressiveMean:
    count_so_far: int
    mean_so_far: float

    def __init__(self):
        self.count_so_far = 0
        self.mean_so_far = 0.0

    def update(self, batch: pd.Series):
        if len(batch) == 0:
            return
        if self.count_so_far == 0:
            self.mean_so_far = float(batch.mean())
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

    def merge_subtotals(self, right: 'ProgressiveMean'):
        if self.count_so_far == 0:
            self.mean_so_far = right.mean_so_far
            self.count_so_far = right.count_so_far
        elif right.count_so_far == 0:
            pass
        else:
            n_a = self.count_so_far
            n_b = right.count_so_far
            n_ab = n_a + n_b
            mean_a = self.mean_so_far
            mean_b = right.mean_so_far
            delta = mean_b - mean_a
            self.mean_so_far += delta * n_b / n_ab
            self.count_so_far = n_ab

    @property
    def mean(self) -> float:
        if self.count_so_far == 0:
            return math.nan
        return self.mean_so_far
