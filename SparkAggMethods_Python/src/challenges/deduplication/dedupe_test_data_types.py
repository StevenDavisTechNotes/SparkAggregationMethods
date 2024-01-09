from dataclasses import dataclass
from typing import Callable, Literal

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from utils.tidy_spark_session import TidySparkSession


@dataclass(frozen=True)
class ExecutionParameters:
    InCloudMode: bool
    NumExecutors: int
    CanAssumeNoDupesPerPartition: bool
    DefaultParallelism: int
    TestDataFolderLocation: str


# region data structure
RecordSparseStruct = DataTypes.StructType([
    DataTypes.StructField("FirstName", DataTypes.StringType(), False),
    DataTypes.StructField("LastName", DataTypes.StringType(), False),
    DataTypes.StructField("StreetAddress", DataTypes.StringType(), True),
    DataTypes.StructField("City", DataTypes.StringType(), True),
    DataTypes.StructField("ZipCode", DataTypes.StringType(), False),
    DataTypes.StructField("SecretKey", DataTypes.IntegerType(), False),
    DataTypes.StructField("FieldA", DataTypes.StringType(), True),
    DataTypes.StructField("FieldB", DataTypes.StringType(), True),
    DataTypes.StructField("FieldC", DataTypes.StringType(), True),
    DataTypes.StructField("FieldD", DataTypes.StringType(), True),
    DataTypes.StructField("FieldE", DataTypes.StringType(), True),
    DataTypes.StructField("FieldF", DataTypes.StringType(), True),
])
# endregion


@dataclass(frozen=True)
class DataSet:
    num_people: int
    num_sources: int
    data_size: int
    grouped_num_partitions: int
    df: spark_DataFrame


TPysparkPythonPendingAnswerSet = Literal["infeasible"] | RDD[Row] | spark_DataFrame


@dataclass(frozen=True)
class PysparkTestMethod:
    original_strategy_name: str
    strategy_name: str
    language: str
    interface: str
    delegate: Callable[
        [TidySparkSession, ExecutionParameters, DataSet],
        TPysparkPythonPendingAnswerSet]


@dataclass(frozen=True)
class ItineraryItem:
    test_method: PysparkTestMethod
    data_set: DataSet
