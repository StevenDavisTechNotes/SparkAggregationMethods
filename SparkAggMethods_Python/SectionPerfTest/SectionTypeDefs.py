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
    dataSize: int
    filename: str
    sectionMaximum: int
    NumStudents: int


@dataclass(frozen=True)
class RunResult:
    success: bool
    data: DataSetDescription
    elapsed_time: float
    record_count: int
# RunResult = collections.namedtuple(
#     "RunResult", ["dataSize", "SectionMaximum", "elapsedTime", "recordCount"])


@dataclass(frozen=True)
class PythonTestMethod:
    strategy_name: str
    language: str
    interface: str
    scale: str
    delegate: Callable[
        [TidySparkSession, DataSetDescription],
        Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]]
