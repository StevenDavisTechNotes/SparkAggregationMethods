import collections
from dataclasses import dataclass

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from Utils.SparkUtils import TidySparkSession

from .DedupeDirectory import PythonTestMethod
from .DedupeDataTypes import RecordSparseStruct, nameHash
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from .DedupeDataTypes import ExecutionParameters


@dataclass(frozen=True)
class ExecutionParameters:
    in_cloud_mode: bool
    NumExecutors: int
    CanAssumeNoDupesPerPartition: bool
    DefaultParallelism: int
    SufflePartitions: int
    test_data_file_location: str


@dataclass(frozen=True)
class DataPoint():
    FirstName: str
    LastName: str
    StreetAddress: Optional[str]
    City: str
    ZipCode: str
    SecretKey: int
    FieldA: Optional[str]
    FieldB: Optional[str]
    FieldC: Optional[str]
    FieldD: Optional[str]
    FieldE: Optional[str]
    FieldF: Optional[str]


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

# GenDataSets = collections.namedtuple("GenDataSets", ['NumPeople', 'DataSets'])
# GenDataSet = collections.namedtuple(
#     "GenDataSet", ['NumSources', 'DataSize', 'dfSrc'])


@dataclass(frozen=True)
class DataSetOfSizeOfSources:
    num_sources: int
    data_size: int
    df: spark_DataFrame


@dataclass(frozen=True)
class DataSetsOfSize:
    num_people: int
    data_sets: List[DataSetOfSizeOfSources]


@dataclass(frozen=True)
class PythonTestMethod:
    strategy_name: str
    language: str
    interface: str
    delegate: Callable[
        [TidySparkSession, ExecutionParameters, DataSetOfSizeOfSources],
        Tuple[Optional[RDD], Optional[spark_DataFrame]]]


@dataclass(frozen=True)
class ItineraryItem:
    testMethod: PythonTestMethod
    data_set: DataSetOfSizeOfSources
    data_sets_of_size: DataSetsOfSize
