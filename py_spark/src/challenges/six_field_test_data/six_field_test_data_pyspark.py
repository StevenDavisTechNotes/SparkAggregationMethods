import pyspark.sql.types as DataTypes

DataPointSchema = DataTypes.StructType([
    DataTypes.StructField('id', DataTypes.IntegerType(), False),
    DataTypes.StructField('grp', DataTypes.IntegerType(), False),
    DataTypes.StructField('subgrp', DataTypes.IntegerType(), False),
    DataTypes.StructField('A', DataTypes.IntegerType(), False),
    DataTypes.StructField('B', DataTypes.IntegerType(), False),
    DataTypes.StructField('C', DataTypes.DoubleType(), False),
    DataTypes.StructField('D', DataTypes.DoubleType(), False),
    DataTypes.StructField('E', DataTypes.DoubleType(), False),
    DataTypes.StructField('F', DataTypes.DoubleType(), False)])
