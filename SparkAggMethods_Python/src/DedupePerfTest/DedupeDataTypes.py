from dataclasses import dataclass
from typing import Callable, Optional, Tuple

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession


@dataclass(frozen=True)
class ExecutionParameters:
    InCloudMode: bool
    NumExecutors: int
    CanAssumeNoDupesPerPartition: bool
    DefaultParallelism: int
    TestDataFolderLocation: str


# @dataclass(frozen=True)
# class DataPoint():
#     FirstName: str
#     LastName: str
#     StreetAddress: Optional[str]
#     City: str
#     ZipCode: str
#     SecretKey: int
#     FieldA: Optional[str]
#     FieldB: Optional[str]
#     FieldC: Optional[str]
#     FieldD: Optional[str]
#     FieldE: Optional[str]
#     FieldF: Optional[str]


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


@dataclass(frozen=True)
class PythonTestMethod:
    strategy_name: str
    language: str
    interface: str
    delegate: Callable[
        [TidySparkSession, ExecutionParameters, DataSet],
        Tuple[Optional[RDD], Optional[spark_DataFrame]]]


@dataclass(frozen=True)
class ItineraryItem:
    test_method: PythonTestMethod
    data_set: DataSet
