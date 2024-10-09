
from typing import NamedTuple

import pyspark.sql.types as DataTypes

from src.six_field_test_data.six_test_data_types import DataPointSchema, DataSetDescription

GROUP_BY_COLUMNS = ['grp', 'subgrp']
AGGREGATION_COLUMNS_3 = ['mean_of_C', 'max_of_D', 'cond_var_of_E']
POST_AGGREGATION_SCHEMA_3 = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in GROUP_BY_COLUMNS] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in AGGREGATION_COLUMNS_3])

AGGREGATION_COLUMNS_4 = ['mean_of_C', 'max_of_D', 'cond_var_of_E', 'cond_var_of_E2']
POST_AGGREGATION_SCHEMA_4 = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in GROUP_BY_COLUMNS] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in AGGREGATION_COLUMNS_4])


DATA_SIZES_LIST_CONDITIONAL = [
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
        (10 ** 7, '3_3_10m'),
        (10 ** 8, '3_3_100m'),
    ]
]


class SubTotal(NamedTuple):
    running_sum_of_C: float
    running_uncond_count: int
    running_max_of_D: float
    running_cond_sum_of_E_squared: float
    running_cond_sum_of_E: float
    running_cond_count: int
