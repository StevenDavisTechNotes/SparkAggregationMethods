import pyspark.sql.types as DataTypes

from src.six_field_test_data.six_test_data_types import (DataPointSchema,
                                                         DataSetDescription)
from src.utils.spark_helpers import make_empty_pd_dataframe_from_spark_types

GROUP_BY_COLUMNS = ['grp', 'subgrp']
AGGREGATION_COLUMNS_NON_NULL = ['mean_of_C', 'max_of_D']
AGGREGATION_COLUMNS_NULLABLE = ['var_of_E', 'var_of_E2']
AGGREGATION_COLUMNS = AGGREGATION_COLUMNS_NON_NULL + AGGREGATION_COLUMNS_NULLABLE
RESULT_COLUMNS = GROUP_BY_COLUMNS + AGGREGATION_COLUMNS
pyspark_post_agg_schema = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in GROUP_BY_COLUMNS]
    + [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in AGGREGATION_COLUMNS_NON_NULL]
    + [DataTypes.StructField(name, DataTypes.DoubleType(), True)
        for name in AGGREGATION_COLUMNS_NULLABLE])
dask_post_agg_schema = make_empty_pd_dataframe_from_spark_types(pyspark_post_agg_schema)


DATA_SIZES_LIST_VANILLA = [
    DataSetDescription(
        size_code=code,
        num_grp_1=3,
        num_grp_2=3,
        points_per_index=scale,
    )
    for scale, code in [
        (10 ** 0, '3_3_1'),
        (10 ** 1, '3_3_10'),
        (10 ** 2, '3_3_100'),
        (10 ** 3, '3_3_1k'),
        (10 ** 4, '3_3_10k'),
        (10 ** 5, '3_3_100k'),
        (10 ** 6, '3_3_1m'),
        # (10 ** 7, '3_3_10m'),
        # (10 ** 8, '3_3_100m'),
    ]
]
