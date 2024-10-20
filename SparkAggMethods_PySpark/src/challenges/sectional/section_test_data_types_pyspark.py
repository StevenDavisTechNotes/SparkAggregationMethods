import inspect
from dataclasses import dataclass
from enum import StrEnum
from typing import Callable, Iterable, Literal, NamedTuple, Protocol

import pandas as pd
import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, DataSetDescriptionBase, SolutionInterfaceDask,
    SolutionInterfacePySpark, SolutionInterfacePythonOnly, SolutionLanguage, TChallengeMethodDelegate,
    TSolutionInterface,
)

from src.utils.tidy_session_pyspark import TidySparkSession


@dataclass(frozen=True)
class ExecutionParameters:
    default_parallelism: int
    maximum_processable_segment: int
    test_data_folder_location: str


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


class SectionDataSetDescription(DataSetDescriptionBase):
    # for DataSetDescriptionBase
    debugging_only: bool
    num_source_rows: int
    size_code: str
    # for SectionDataSetDescription
    i_scale: int
    num_students: int
    section_size_max: int

    def __init__(
            self,
            *,
            i_scale: int,
            num_students: int,
            section_size_max: int,
    ) -> None:
        debugging_only = False
        num_source_rows = num_students * section_size_max
        size_code = str(num_students)
        super().__init__(
            debugging_only=debugging_only,
            num_source_rows=num_source_rows,
            size_code=size_code,
        )
        self.i_scale = i_scale
        self.num_students = num_students
        self.section_size_max = section_size_max

    @classmethod
    def regressor_field_name(cls) -> str:
        regressor_field_name = "num_students"
        assert regressor_field_name in inspect.get_annotations(cls)
        return regressor_field_name


@dataclass(frozen=True)
class SectionPySparkExecutionParameters(ExecutionParameters):
    target_num_partitions: int
    section_maximum: int
    source_data_file_path: str


@dataclass(frozen=True)
class SectionDataSet():
    data_description: SectionDataSetDescription
    exec_params: SectionPySparkExecutionParameters


@dataclass(frozen=True)
class SectionDataSetWithAnswer(SectionDataSet):
    answer_generator: Callable[[], Iterable[StudentSummary]] | None


class SolutionScale(StrEnum):
    WHOLE_FILE = 'whole_file'
    WHOLE_SECTION = 'whole_section'
    THREE_ROWS = 'three_rows'
    FINAL_SUMMARIES = 'final_summaries'
    SINGLE_LINE = 'singleline'


@dataclass(frozen=True)
class SectionChallengeMethodRegistrationBase(
    ChallengeMethodRegistrationBase[
        TSolutionInterface, TChallengeMethodDelegate
    ]
):
    # for ChallengeMethodRegistrationBase
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: TSolutionInterface
    requires_gpu: bool
    delegate: TChallengeMethodDelegate
    # for SixTestDataChallengeMethodRegistrationBase
    scale: SolutionScale


TChallengePythonAnswer = (
    Literal["infeasible"] | list[StudentSummary] | pd.DataFrame)

TChallengePythonPysparkAnswer = (
    Literal["infeasible"] | list[StudentSummary] | RDD[StudentSummary] | PySparkDataFrame)


class ISectionChallengeMethodPythonDask(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        data_set: SectionDataSet
    ) -> TChallengePythonAnswer: ...


@dataclass(frozen=True)
class ChallengeMethodDaskRegistration(
    SectionChallengeMethodRegistrationBase[
        SolutionInterfaceDask, ISectionChallengeMethodPythonDask
    ]
):
    interface: SolutionInterfaceDask
    delegate: ISectionChallengeMethodPythonDask


class ISectionChallengeMethodPythonPyspark(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        data_set: SectionDataSet
    ) -> TChallengePythonPysparkAnswer: ...


@dataclass(frozen=True)
class SectionChallengeMethodPysparkRegistration(
    SectionChallengeMethodRegistrationBase[SolutionInterfacePySpark, ISectionChallengeMethodPythonPyspark]
):
    strategy_name_2018: str
    interface: SolutionInterfacePySpark
    delegate: ISectionChallengeMethodPythonPyspark


class ISectionChallengeMethodPythonOnly(Protocol):
    def __call__(
        self,
        *,
        data_set: SectionDataSet
    ) -> TChallengePythonAnswer: ...


@dataclass(frozen=True)
class SectionChallengeMethodPythonOnlyRegistration(
    SectionChallengeMethodRegistrationBase[SolutionInterfacePythonOnly, ISectionChallengeMethodPythonOnly]
):
    interface: SolutionInterfacePythonOnly
    delegate: ISectionChallengeMethodPythonOnly
