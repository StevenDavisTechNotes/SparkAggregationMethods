from dataclasses import dataclass
from typing import Literal, Protocol

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.deduplication.dedupe_test_data_types import (
    DedupeDataSetBase, DedupeDataSetDescription, ExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, SolutionInterfacePySpark, SolutionLanguage,
)

from src.utils.tidy_session_pyspark import TidySparkSession

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


@dataclass(frozen=True)
class DedupePySparkDataSet(DedupeDataSetBase):
    data_description: DedupeDataSetDescription
    grouped_num_partitions: int
    df: PySparkDataFrame


TChallengePendingAnswerPythonPyspark = Literal["infeasible"] | RDD[Row] | PySparkDataFrame


class IChallengeMethodPythonPyspark(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DedupePySparkDataSet
    ) -> TChallengePendingAnswerPythonPyspark: ...


@dataclass(frozen=True)
class DedupeChallengeMethodPythonPysparkRegistration(
    ChallengeMethodRegistrationBase[SolutionInterfacePySpark, IChallengeMethodPythonPyspark]
):
    # for ChallengeMethodRegistrationBase
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfacePySpark
    requires_gpu: bool
    delegate: IChallengeMethodPythonPyspark


@dataclass(frozen=True)
class DedupeItineraryItem:
    challenge_method_registration: DedupeChallengeMethodPythonPysparkRegistration
    data_set: DedupePySparkDataSet
