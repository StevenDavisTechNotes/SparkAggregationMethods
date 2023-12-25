from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    PandasSeriesOfFloat = pd.Series[float]
    PandasSeriesOfInt = pd.Series[int]
else:
    PandasSeriesOfFloat = pd.Series
    PandasSeriesOfInt = pd.Series
