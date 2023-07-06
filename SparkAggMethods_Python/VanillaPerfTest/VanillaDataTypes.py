import pyspark.sql.types as DataTypes

from SixFieldTestData import DataPointSchema

groupby_columns = ['grp', 'subgrp']
agg_columns_non_null = ['mean_of_C', 'max_of_D']
agg_columns_nullable = ['var_of_E', 'var_of_E2']
agg_columns = agg_columns_non_null + agg_columns_nullable
postAggSchema = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in groupby_columns]
    + [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in agg_columns_non_null]
    + [DataTypes.StructField(name, DataTypes.DoubleType(), True)
        for name in agg_columns_nullable])