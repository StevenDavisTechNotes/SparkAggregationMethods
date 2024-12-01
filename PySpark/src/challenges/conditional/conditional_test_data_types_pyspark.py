import pyspark.sql.types as DataTypes
from spark_agg_methods_common_python.challenges.conditional.conditional_test_data_types import (
    AGGREGATION_COLUMNS_3, AGGREGATION_COLUMNS_4, GROUP_BY_COLUMNS,
)
from spark_agg_methods_common_python.perf_test_common import Challenge

from src.challenges.six_field_test_data.six_field_test_data_pyspark import DataPointSchema

CHALLENGE = Challenge.CONDITIONAL
POST_AGGREGATION_SCHEMA_3 = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in GROUP_BY_COLUMNS] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in AGGREGATION_COLUMNS_3])
POST_AGGREGATION_SCHEMA_4 = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in GROUP_BY_COLUMNS] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in AGGREGATION_COLUMNS_4])
