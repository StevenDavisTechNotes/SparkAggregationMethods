import os
import re

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from SectionPerfTest.SectionTestData import TEST_DATA_FILE_LOCATION
from SectionPerfTest.SectionTypeDefs import (
    ClassLine, LabeledTypedRow, SparseLineSchema, StudentHeader, StudentSummary, TrimesterFooter, TrimesterHeader)
from Utils.SparkUtils import TidySparkSession

# region parsers


def parseLineToTypes(
        line: str,
) -> StudentHeader | TrimesterHeader | ClassLine | TrimesterFooter:
    fields = line.rstrip().split(',')
    if fields[0] == 'S':
        return StudentHeader(StudentId=int(fields[1]), StudentName=fields[2])
    if fields[0] == 'TH':
        return TrimesterHeader(Date=fields[1], WasAbroad=(fields[2] == 'True'))
    if fields[0] == 'C':
        return ClassLine(Dept=int(fields[1]), Credits=int(
            fields[2]), Grade=int(fields[3]))
    if fields[0] == 'TF':
        return TrimesterFooter(Major=int(fields[1]), GPA=float(
            fields[2]), Credits=int(fields[3]))
    raise Exception("Malformed data " + line)


def parseLineToRow(
        line: str,
) -> Row:
    fields = line.split(',')
    if fields[0] == 'S':
        return Row(Type=fields[0],
                   StudentId=int(fields[1]), StudentName=fields[2],
                   Date=None, WasAbroad=None,
                   Dept=None, ClassCredits=None, ClassGrade=None,
                   Major=None, TriGPA=None, TriCredits=None)
    if fields[0] == 'TH':
        return Row(Type=fields[0],
                   StudentId=None, StudentName=None,
                   Date=fields[1], WasAbroad=(fields[2] == 'True'),
                   Dept=None, ClassCredits=None, ClassGrade=None,
                   Major=None, TriGPA=None, TriCredits=None)
    if fields[0] == 'C':
        return Row(Type=fields[0],
                   StudentId=None, StudentName=None,
                   Date=None, WasAbroad=None,
                   Dept=int(fields[1]), ClassCredits=int(fields[2]), ClassGrade=int(fields[3]),
                   Major=None, TriGPA=None, TriCredits=None)
    if fields[0] == 'TF':
        return Row(Type=fields[0],
                   StudentId=None, StudentName=None,
                   Date=None, WasAbroad=None,
                   Dept=None, ClassCredits=None, ClassGrade=None,
                   Major=int(fields[1]), TriGPA=float(fields[2]), TriCredits=int(fields[3]))
    raise Exception("Malformed data " + line)


def dfSparseRowsFactory(
        spark_session: TidySparkSession,
        filename: str,
        numPartitions: int | None = None
) -> spark_DataFrame:
    rdd = spark_session.spark_context.textFile(
        filename, minPartitions=(numPartitions or 1))
    rdd = rdd \
        .map(parseLineToRow)
    df = spark_session.spark.createDataFrame(rdd, SparseLineSchema)
    return df


def rddTypedWithIndexFactory(
        spark_session: TidySparkSession,
        filename: str,
        numPartitions: int | None = None
) -> RDD[LabeledTypedRow]:
    rdd = spark_session.spark_context.textFile(
        filename, minPartitions=(numPartitions or 1))
    rddTypedWithIndex = rdd \
        .map(parseLineToTypes) \
        .zipWithIndex() \
        .map(lambda pair:
             LabeledTypedRow(
                 Index=pair[1],
                 Value=pair[0]))
    return rddTypedWithIndex

# endregion
# region aggregators


def rowToStudentSummary(
        x: Row
) -> StudentSummary:
    return StudentSummary(
        StudentId=x.StudentId,
        StudentName=x.StudentName,
        SourceLines=x.SourceLines,
        Major=x.Major,
        GPA=x.GPA,
        MajorGPA=x.MajorGPA)


# endregion
# region Preprocessor


def identifySectionUsingIntermediateFile(
        srcFilename: str,
) -> str:
    destFilename = f"{TEST_DATA_FILE_LOCATION}/temp.csv"
    if os.path.exists(destFilename):
        os.unlink(destFilename)
    reExtraType = re.compile("^S,")
    sectionId = -1
    with open(destFilename, "w") as outf:
        with open(srcFilename, "r") as inf:
            for line in inf:
                if reExtraType.match(line):
                    sectionId += 1
                assert sectionId >= 0
                outf.write(f"{sectionId},{line}")
    return destFilename
# endregion
