
from typing import NamedTuple

import pyspark.sql.types as DataTypes

from SixFieldCommon.SixFieldTestData import DataPointSchema

groupby_columns = ['grp', 'subgrp']
agg_columns_3 = ['mean_of_C', 'max_of_D', 'cond_var_of_E']
postAggSchema_3 = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in groupby_columns] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in agg_columns_3])

groupby_columns = ['grp', 'subgrp']
agg_columns_4 = ['mean_of_C', 'max_of_D', 'cond_var_of_E', 'cond_var_of_E2']
postAggSchema_4 = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in groupby_columns] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in agg_columns_4])


class SubTotal(NamedTuple):
    running_sum_of_C: float
    running_uncond_count: int
    running_max_of_D: float | None
    running_cond_sum_of_E_squared: float
    running_cond_sum_of_E: float
    running_cond_count: int
