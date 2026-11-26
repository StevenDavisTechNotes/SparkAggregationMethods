from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

import pandas as pd
import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from spark_agg_methods_common_python.challenges.sectional.sectional_test_data_types import (
    SectionalChallengeMethodRegistrationBase, SectionalDataSetBase, SectionalExecutionParametersBase, StudentSummary,
)
from spark_agg_methods_common_python.perf_test_common import SolutionInterfacePySpark
from src.utils.tidy_session_pyspark import TidySparkSession

MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT_SECTIONAL_PYSPARK: int = 5
MAXIMUM_PROCESSABLE_SEGMENT_SECTIONAL_PYSPARK: int = 10**MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT_SECTIONAL_PYSPARK
# On Windows every task launches its own Python worker on a fresh loopback port (no worker daemon), so a stage with
# thousands of tasks exhausts the ephemeral port range ("No buffer space available ... bind"). Strategies whose
# stages are sized by target_num_partitions report "infeasible" above this many partitions.
MAX_PARTITIONS_PER_STAGE_SECTIONAL_PYSPARK: int = 10**3

StudentSummaryStruct = DataTypes.StructType([
    DataTypes.StructField("StudentId", DataTypes.IntegerType(), True),
    DataTypes.StructField("StudentName", DataTypes.StringType(), True),
    DataTypes.StructField("SourceLines", DataTypes.IntegerType(), True),
    DataTypes.StructField("GPA", DataTypes.DoubleType(), True),
    DataTypes.StructField("Major", DataTypes.StringType(), True),
    DataTypes.StructField("MajorGPA", DataTypes.DoubleType(), True),
])
SparseLineSchema = DataTypes.StructType([
    DataTypes.StructField("Type", DataTypes.StringType(), True),
    DataTypes.StructField("StudentId", DataTypes.IntegerType(), True),
    DataTypes.StructField("StudentName", DataTypes.StringType(), True),
    DataTypes.StructField("Date", DataTypes.StringType(), True),
    DataTypes.StructField("WasAbroad", DataTypes.BooleanType(), True),
    DataTypes.StructField("Dept", DataTypes.IntegerType(), True),
    DataTypes.StructField("ClassCredits", DataTypes.IntegerType(), True),
    DataTypes.StructField("ClassGrade", DataTypes.IntegerType(), True),
    DataTypes.StructField("Major", DataTypes.IntegerType(), True),
    DataTypes.StructField("TriGPA", DataTypes.DoubleType(), True),
    DataTypes.StructField("TriCredits", DataTypes.IntegerType(), True),
])


@dataclass(frozen=True)
class SectionalDataSetPyspark(SectionalDataSetBase):
    maximum_processable_segment: int
    source_data_file_path: Path
    target_num_partitions: int


TChallengePythonPysparkAnswer = (
    tuple[Literal["infeasible"], str]
    | list[StudentSummary]
    | RDD[StudentSummary]
    | PySparkDataFrame
    | pd.DataFrame
)


@dataclass(frozen=True)
class SectionalExecutionParametersPyspark(SectionalExecutionParametersBase):
    maximum_processable_segment: int


class ISectionalChallengeMethodPythonPyspark(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        exec_params: SectionalExecutionParametersPyspark,
        data_set: SectionalDataSetPyspark,
    ) -> TChallengePythonPysparkAnswer: ...


@dataclass(frozen=True)
class SectionalChallengeMethodPysparkRegistration(
    SectionalChallengeMethodRegistrationBase[SolutionInterfacePySpark, ISectionalChallengeMethodPythonPyspark]
):
    strategy_name_2018: str
    strategy_name_2023: str | None
    interface: SolutionInterfacePySpark
    delegate: ISectionalChallengeMethodPythonPyspark
