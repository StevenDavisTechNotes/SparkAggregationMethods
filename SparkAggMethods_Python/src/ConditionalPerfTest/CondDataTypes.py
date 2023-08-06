
import pyspark.sql.types as DataTypes

from SixFieldCommon.SixFieldTestData import DataPointSchema
import collections


SubTotal = collections.namedtuple(
    "SubTotal",
    ["running_sum_of_C", "running_uncond_count", "running_max_of_D",
     "running_cond_sum_of_E_squared", "running_cond_sum_of_E",
     "running_cond_count"])

GrpTotal = collections.namedtuple(
    "GrpTotal", ["grp", "subgrp", "mean_of_C", "max_of_D", "cond_var_of_E"])

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
