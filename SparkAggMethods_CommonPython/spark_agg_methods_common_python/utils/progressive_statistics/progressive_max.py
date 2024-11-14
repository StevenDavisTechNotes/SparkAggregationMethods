import math

import pandas as pd


class ProgressiveMax:
    max_so_far: float | None

    def __init__(self):
        self.max_so_far = None

    def update(self, batch: pd.Series):
        if len(batch) == 0:
            return
        new_max = batch.max()
        if self.max_so_far is None or new_max > self.max_so_far:
            self.max_so_far = new_max

    def merge_subtotals(self, right: 'ProgressiveMax'):
        if self.max_so_far is None:
            self.max_so_far = right.max_so_far
        elif right.max_so_far is None:
            pass
        else:
            self.max_so_far = max(self.max_so_far, right.max_so_far)

    @property
    def max(self) -> float:
        return self.max_so_far or math.nan
