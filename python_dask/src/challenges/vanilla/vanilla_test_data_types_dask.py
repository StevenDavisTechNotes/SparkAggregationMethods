import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    AGGREGATION_COLUMNS_NON_NULL, AGGREGATION_COLUMNS_NULLABLE,
)

POST_AGG_SCHEMA_NO_INDEX_DASK = pd.DataFrame(numpy.empty(0, dtype=numpy.dtype(
    [(x, numpy.float64) for x in AGGREGATION_COLUMNS_NON_NULL]
    + [(x, numpy.float64) for x in AGGREGATION_COLUMNS_NULLABLE]
)))
