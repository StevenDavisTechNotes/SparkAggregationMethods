from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd
import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    SectionChallengeMethodRegistrationBase, SectionDataSetBase, SectionExecutionParametersBase, StudentSummary,
)
from spark_agg_methods_common_python.perf_test_common import SolutionInterfacePySpark

from src.utils.tidy_session_pyspark import TidySparkSession

MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT: int = 5
MAXIMUM_PROCESSABLE_SEGMENT: int = 10**MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT

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
class SectionDataSetPyspark(SectionDataSetBase):
    maximum_processable_segment: int
    source_data_file_path: str
    target_num_partitions: int


TChallengePythonPysparkAnswer = (
    tuple[Literal["infeasible"], str]
    | list[StudentSummary]
    | RDD[StudentSummary]
    | PySparkDataFrame
    | pd.DataFrame
)


@dataclass(frozen=True)
class SectionExecutionParametersPyspark(SectionExecutionParametersBase):
    maximum_processable_segment: int


class ISectionChallengeMethodPythonPyspark(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        exec_params: SectionExecutionParametersPyspark,
        data_set: SectionDataSetPyspark,
    ) -> TChallengePythonPysparkAnswer: ...


@dataclass(frozen=True)
class SectionChallengeMethodPysparkRegistration(
    SectionChallengeMethodRegistrationBase[SolutionInterfacePySpark, ISectionChallengeMethodPythonPyspark]
):
    strategy_name_2018: str
    interface: SolutionInterfacePySpark
    delegate: ISectionChallengeMethodPythonPyspark
