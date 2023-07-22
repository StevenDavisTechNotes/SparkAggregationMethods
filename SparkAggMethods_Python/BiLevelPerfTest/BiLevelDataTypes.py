import pyspark.sql.types as DataTypes

from SixFieldCommon.SixFieldTestData import DataPointSchema

groupby_columns = ['grp']
agg_columns = ['mean_of_C', 'max_of_D', 'avg_var_of_E', 'avg_var_of_E2']
postAggSchema = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in groupby_columns] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in agg_columns])
