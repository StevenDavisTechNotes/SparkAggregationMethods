from dataclasses import dataclass
from typing import Callable, Literal, Protocol

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row

from src.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, SolutionInterface, SolutionInterfacePySpark, SolutionLanguage,
)
from src.utils.tidy_spark_session import TidySparkSession


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
    df: PySparkDataFrame


TChallengePendingAnswerPythonPyspark = Literal["infeasible"] | RDD[Row] | PySparkDataFrame


class IChallengeMethodPythonPyspark(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSet
    ) -> TChallengePendingAnswerPythonPyspark: ...


@dataclass(frozen=True)
class ChallengeMethodPythonPysparkRegistration(ChallengeMethodRegistrationBase):
    strategy_name_2018: str
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfacePySpark
    requires_gpu: bool
    delegate: IChallengeMethodPythonPyspark

    @property
    def interface_getter(self) -> SolutionInterface:
        return self.interface

    @property
    def delegate_getter(self) -> Callable | None:
        return self.delegate


@dataclass(frozen=True)
class ItineraryItem:
    challenge_method_registration: ChallengeMethodPythonPysparkRegistration
    data_set: DataSet
