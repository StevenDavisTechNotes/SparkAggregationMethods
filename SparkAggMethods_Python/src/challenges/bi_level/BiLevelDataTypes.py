import pyspark.sql.types as DataTypes

from six_field_test_data.six_test_data_types import DataPointSchema

groupby_columns = ['grp']
agg_columns = ['mean_of_C', 'max_of_D', 'avg_var_of_E', 'avg_var_of_E2']
result_columns = groupby_columns + agg_columns
postAggSchema = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in groupby_columns] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in agg_columns])
