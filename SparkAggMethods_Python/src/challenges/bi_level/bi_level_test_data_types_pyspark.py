import pyspark.sql.types as DataTypes
from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import (
    AGGREGATION_COLUMNS, GROUP_BY_COLUMNS,
)

from src.challenges.six_field_test_data.six_field_pyspark_test_data import DataPointSchema

PysparkPostAggregationSchema = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in GROUP_BY_COLUMNS] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in AGGREGATION_COLUMNS])
