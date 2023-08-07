import collections
from dataclasses import dataclass
from typing import Callable, List, Tuple

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession


@dataclass(frozen=True)
class ExecutionParameters:
    DefaultParallelism: int
    MaximumProcessableSegment: int
    TestDataFolderLocation: str


# region GenData
StudentHeader = collections.namedtuple("StudentHeader",
                                       ["StudentId", "StudentName"])
TrimesterHeader = collections.namedtuple("TrimesterHeader",
                                         ["Date", "WasAbroad"])
ClassLine = collections.namedtuple("ClassLine",
                                   ["Dept", "Credits", "Grade"])
TrimesterFooter = collections.namedtuple("TrimesterFooter",
                                         ["Major", "GPA", "Credits"])
TypedLine = StudentHeader | TrimesterHeader | ClassLine | TrimesterFooter

StudentSummary = collections.namedtuple("StudentSummary",
                                        ["StudentId", "StudentName", "SourceLines", "GPA", "Major", "MajorGPA"])
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
LabeledTypedRow = collections.namedtuple(
    "LabeledTypedRow",
    ["Index", "Value"])
NumDepts = 4
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
class DataSetAnswer():
    correct_answer: List[StudentSummary]


@dataclass(frozen=True)
class DataSet():
    description: DataSetDescription
    data: DataSetData
    exec_params: ExecutionParameters


@dataclass(frozen=True)
class DataSetWithAnswer(DataSet):
    answer: DataSetAnswer


@dataclass(frozen=True)
class RunResult:
    success: bool
    data: DataSet
    elapsed_time: float
    record_count: int


@dataclass(frozen=True)
class PythonTestMethod:
    strategy_name: str
    language: str
    interface: str
    scale: str
    delegate: Callable[
        [TidySparkSession, DataSet],
        Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]]
