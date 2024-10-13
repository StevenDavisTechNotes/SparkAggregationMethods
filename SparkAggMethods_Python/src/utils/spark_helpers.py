
import numpy
import pandas as pd
import pyspark.sql.types as DataTypes
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as PySparkDataFrame

# based on https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex/32741497#32741497


def zip_dataframe_with_index(
    df, spark: SparkSession, offset: int = 1,
    colName: str = "rowId"
) -> PySparkDataFrame:
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''

    new_schema = DataTypes.StructType(
        [DataTypes.StructField(
            colName, DataTypes.LongType(), True)]
        + df.schema.fields)

    zipped_rdd = df.rdd.zipWithIndex()

    new_rdd = zipped_rdd.map(
        lambda kv: ([kv[1] + offset] + list(kv[0])))

    return spark.createDataFrame(new_rdd, new_schema)


def translate_spark_datatype_to_numpy(
    spark_datatype: DataTypes.DataType
) -> numpy.dtype:
    if spark_datatype == DataTypes.StringType():
        return numpy.dtype(str)
    elif spark_datatype == DataTypes.IntegerType():
        return numpy.dtype(int)
    elif spark_datatype == DataTypes.DoubleType():
        return numpy.dtype(float)
    elif spark_datatype == DataTypes.TimestampType():
        return numpy.dtype(numpy.datetime64)
    else:
        raise ValueError(
            f"translate_spark_datatype_to_numpy: unsupported type {spark_datatype}")


def make_empty_pd_dataframe_from_spark_types(
    spark_schema: DataTypes.StructType
) -> pd.DataFrame:
    dtypes = numpy.dtype(
        [(x.name, translate_spark_datatype_to_numpy(x.dataType)) for x in spark_schema.fields]
    )
    df = pd.DataFrame(numpy.empty(0, dtype=dtypes))
    return df
