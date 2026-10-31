import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.conditional.conditional_test_data_types import AGGREGATION_COLUMNS_4

POST_AGG_SCHEMA_NO_INDEX_DASK = pd.DataFrame(numpy.empty(0, dtype=numpy.dtype(
    [(x, numpy.float64) for x in AGGREGATION_COLUMNS_4]
)))
