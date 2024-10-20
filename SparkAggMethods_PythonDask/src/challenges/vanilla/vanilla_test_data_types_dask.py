import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    AGGREGATION_COLUMNS_NON_NULL, AGGREGATION_COLUMNS_NULLABLE, GROUP_BY_COLUMNS,
)

dask_post_agg_schema = pd.DataFrame(numpy.empty(0, dtype=numpy.dtype(
    [(x, numpy.dtype(str)) for x in GROUP_BY_COLUMNS]
    + [(x, numpy.dtype(float)) for x in AGGREGATION_COLUMNS_NON_NULL]
    + [(x, numpy.dtype(float)) for x in AGGREGATION_COLUMNS_NULLABLE]
)))
