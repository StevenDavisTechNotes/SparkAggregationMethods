import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.bilevel.bilevel_test_data_types import AGGREGATION_COLUMNS

POST_AGG_SCHEMA_NO_INDEX_DASK = pd.DataFrame(numpy.empty(0, dtype=numpy.dtype(
    [(x, numpy.float64) for x in AGGREGATION_COLUMNS]
)))
