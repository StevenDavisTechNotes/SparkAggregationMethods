from dataclasses import dataclass
from enum import StrEnum
from typing import Callable, Iterable, Literal, NamedTuple, Protocol

import pandas as pd
import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame

from src.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, SolutionInterface, SolutionInterfaceDask, SolutionInterfacePySpark,
    SolutionInterfacePythonOnly, SolutionLanguage,
)
from src.utils.tidy_spark_session import TidySparkSession


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
    num_students: int
    section_size_max: int

    @property
    def size_code(self) -> str:
        return str(self.num_students)

    @property
    def num_rows(self) -> int:
        return self.num_students * self.section_size_max


@dataclass(frozen=True)
class DataSetData:
    target_num_partitions: int
    section_maximum: int
    test_filepath: str


@dataclass(frozen=True)
class DataSet():
    data_size: DataSetDescription
    data: DataSetData
    exec_params: ExecutionParameters


@dataclass(frozen=True)
class DataSetWithAnswer(DataSet):
    answer_generator: Callable[[], Iterable[StudentSummary]] | None


@dataclass(frozen=True)
class RunResult:
    strategy_name: str
    engine: CalcEngine
    success: bool
    data: DataSet
    elapsed_time: float
    record_count: int


class SolutionScale(StrEnum):
    WHOLE_FILE = 'whole_file'
    WHOLE_SECTION = 'whole_section'
    THREE_ROWS = 'three_rows'
    FINAL_SUMMARIES = 'final_summaries'
    SINGLE_LINE = 'singleline'


@dataclass(frozen=True)
class SectionChallengeMethodRegistrationBase(ChallengeMethodRegistrationBase):
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    # interface: str
    scale: SolutionScale
    requires_gpu: bool


TChallengePythonAnswer = (
    Literal["infeasible"] | list[StudentSummary] | pd.DataFrame)

TChallengePythonPysparkAnswer = (
    Literal["infeasible"] | list[StudentSummary] | RDD[StudentSummary] | PySparkDataFrame)


class IChallengeMethodPythonDaskRegistration(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        data_set: DataSet
    ) -> TChallengePythonAnswer: ...


@dataclass(frozen=True)
class ChallengeMethodDaskRegistration(SectionChallengeMethodRegistrationBase):
    interface: SolutionInterfaceDask
    delegate: IChallengeMethodPythonDaskRegistration

    @property
    def delegate_getter(self) -> Callable:
        return self.delegate

    @property
    def interface_getter(self) -> SolutionInterface:
        return self.interface


class IChallengeMethodPythonPysparkRegistration(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        data_set: DataSet
    ) -> TChallengePythonPysparkAnswer: ...


@dataclass(frozen=True)
class ChallengeMethodPysparkRegistration(SectionChallengeMethodRegistrationBase):
    strategy_name_2018: str
    interface: SolutionInterfacePySpark
    delegate: IChallengeMethodPythonPysparkRegistration

    @property
    def delegate_getter(self) -> Callable:
        return self.delegate

    @property
    def interface_getter(self) -> SolutionInterface:
        return self.interface


class IChallengeMethodPythonOnlyRegistration(Protocol):
    def __call__(
        self,
        *,
        data_set: DataSet
    ) -> TChallengePythonAnswer: ...


@dataclass(frozen=True)
class ChallengeMethodPythonOnlyRegistration(SectionChallengeMethodRegistrationBase):
    interface: SolutionInterfacePythonOnly
    delegate: IChallengeMethodPythonOnlyRegistration

    @property
    def delegate_getter(self) -> Callable:
        return self.delegate

    @property
    def interface_getter(self) -> SolutionInterface:
        return self.interface
