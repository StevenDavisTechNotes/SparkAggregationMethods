import pyspark.sql.types as DataTypes
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    AGGREGATION_COLUMNS_NON_NULL, AGGREGATION_COLUMNS_NULLABLE,
    GROUP_BY_COLUMNS,
)
from spark_agg_methods_common_python.perf_test_common import Challenge

from src.challenges.six_field_test_data.six_field_test_data_pyspark import (
    DataPointSchema,
)

CHALLENGE = Challenge.VANILLA
pyspark_post_agg_schema = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in GROUP_BY_COLUMNS]
    + [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in AGGREGATION_COLUMNS_NON_NULL]
    + [DataTypes.StructField(name, DataTypes.DoubleType(), True)
        for name in AGGREGATION_COLUMNS_NULLABLE])
