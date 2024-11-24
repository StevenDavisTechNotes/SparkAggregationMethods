from typing import Self

import pandas as pd


class ProgressiveCount:
    count_so_far: int

    def __init__(self):
        self.count_so_far = 0

    def update_with_population(self, batch: pd.Series | pd.DataFrame):
        self.count_so_far += len(batch)

    def merge_subtotals(self, right: Self):
        self.count_so_far += right.count_so_far

    @property
    def count(self) -> int:
        return self.count_so_far
