from dataclasses import dataclass
from typing import Literal, Protocol

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.deduplication.dedupe_test_data_types import (
    DedupeDataSetBase, DedupeExecutionParametersBase,
)
from spark_agg_methods_common_python.perf_test_common import ChallengeMethodRegistrationBase, SolutionInterfacePySpark

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
class DedupeDataSetPySpark(DedupeDataSetBase):
    grouped_num_partitions: int
    df_source: PySparkDataFrame


TChallengePendingAnswerPythonPyspark = Literal["infeasible"] | RDD[Row] | PySparkDataFrame


@dataclass(frozen=True)
class DedupeExecutionParametersPyspark(DedupeExecutionParametersBase):
    pass


class IChallengeMethodPythonPyspark(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        exec_params: DedupeExecutionParametersPyspark,
        data_set: DedupeDataSetPySpark
    ) -> TChallengePendingAnswerPythonPyspark: ...


@dataclass(frozen=True)
class DedupeChallengeMethodPythonPysparkRegistration(
    ChallengeMethodRegistrationBase[SolutionInterfacePySpark, IChallengeMethodPythonPyspark]
):
    pass
