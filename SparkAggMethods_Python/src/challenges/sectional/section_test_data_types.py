from dataclasses import dataclass
from typing import Callable, Iterable, Literal, NamedTuple, Optional, Protocol

import pandas as pd
import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame

from perf_test_common import CalcEngine
from t_utils.tidy_spark_session import TidySparkSession


@dataclass(frozen=True)
class ExecutionParameters:
    default_parallelism: int
    maximum_processable_segment: int
    test_data_folder_location: str


# region GenData
class StudentHeader(NamedTuple):
    StudentId: int
    StudentName: str


class TrimesterHeader(NamedTuple):
    Date: str
    WasAbroad: bool


class ClassLine(NamedTuple):
    Dept: int
    Credits: int
    Grade: int


class TrimesterFooter(NamedTuple):
    Major: int
    GPA: float
    Credits: int


TypedLine = StudentHeader | TrimesterHeader | ClassLine | TrimesterFooter


class StudentSummary(NamedTuple):
    StudentId: int
    StudentName: str
    SourceLines: int
    GPA: float
    Major: int
    MajorGPA: float


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


class LabeledTypedRow(NamedTuple):
    Index: int
    Value: TypedLine


NumDepartments = 4
# endregion


@dataclass(frozen=True)
class DataSetDescription:
    size_code: str
    num_rows: int
    num_students: int


@dataclass(frozen=True)
class DataSetData:
    target_num_partitions: int
    section_maximum: int
    test_filepath: str


@dataclass(frozen=True)
class DataSet():
    description: DataSetDescription
    data: DataSetData
    exec_params: ExecutionParameters


@dataclass(frozen=True)
class DataSetWithAnswer(DataSet):
    answer_generator: Optional[Callable[[], Iterable[StudentSummary]]]


@dataclass(frozen=True)
class RunResult:
    strategy_name: str
    engine: CalcEngine
    success: bool
    data: DataSet
    elapsed_time: float
    record_count: int


@dataclass(frozen=True)
class TestMethodBase:
    strategy_name: str
    language: str
    interface: str
    scale: str


TChallengeAnswerPythonDask = (
    Literal["infeasible"] | list[StudentSummary] | pd.DataFrame)

TChallengePendingAnswerPythonPyspark = (
    Literal["infeasible"] | list[StudentSummary] | RDD[StudentSummary] | PySparkDataFrame)


class IChallengeMethodPythonDaskRegistration(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        data_set: DataSet
    ) -> TChallengeAnswerPythonDask: ...


@dataclass(frozen=True)
class ChallengeMethodDaskRegistration(TestMethodBase):
    delegate: IChallengeMethodPythonDaskRegistration


class IChallengeMethodPythonPysparkRegistration(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        data_set: DataSet
    ) -> TChallengePendingAnswerPythonPyspark: ...


@dataclass(frozen=True)
class ChallengeMethodPysparkRegistration(TestMethodBase):
    original_strategy_name: str
    delegate: IChallengeMethodPythonPysparkRegistration
