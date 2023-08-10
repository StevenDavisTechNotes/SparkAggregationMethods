
import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as spark_DataFrame


class RddWithNoArgSortByKey:
    def __init__(self, src: RDD) -> None:
        self.src = src

    def sortByKey(self) -> RDD:
        return self.src.sortByKey()


def cast_no_arg_sort_by_key(src: RDD) -> RddWithNoArgSortByKey:
    return RddWithNoArgSortByKey(src)


# from
# https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex/32741497#32741497


def dfZipWithIndex(
    df, spark: SparkSession, offset: int = 1,
    colName: str = "rowId"
) -> spark_DataFrame:
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
