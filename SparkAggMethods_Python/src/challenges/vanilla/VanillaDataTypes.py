import pyspark.sql.types as DataTypes

from six_field_test_data.six_test_data_types import DataPointSchema
from utils.SparkUtils import make_empty_pd_dataframe_from_spark_types

groupby_columns = ['grp', 'subgrp']
agg_columns_non_null = ['mean_of_C', 'max_of_D']
agg_columns_nullable = ['var_of_E', 'var_of_E2']
agg_columns = agg_columns_non_null + agg_columns_nullable
result_columns = groupby_columns + agg_columns
pyspark_post_agg_schema = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in groupby_columns]
    + [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in agg_columns_non_null]
    + [DataTypes.StructField(name, DataTypes.DoubleType(), True)
        for name in agg_columns_nullable])
dask_post_agg_schema = make_empty_pd_dataframe_from_spark_types(pyspark_post_agg_schema)
