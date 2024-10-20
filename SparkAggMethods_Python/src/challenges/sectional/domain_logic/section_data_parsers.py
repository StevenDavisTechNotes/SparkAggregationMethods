import os
import re

from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row

from src.challenges.sectional.section_generate_test_data_pyspark import TEST_DATA_FILE_LOCATION
from src.challenges.sectional.section_test_data_types_pyspark import (
    ClassLine, LabeledTypedRow, SparseLineSchema, StudentHeader, StudentSummary, TrimesterFooter, TrimesterHeader,
)
from src.utils.tidy_spark_session import TidySparkSession

# region parsers


def parse_line_to_types(
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


def parse_line_to_row(
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


def df_sparse_rows_factory(
        spark_session: TidySparkSession,
        filename: str,
        numPartitions: int | None = None
) -> PySparkDataFrame:
    rdd1: RDD[str] = spark_session.spark_context.textFile(
        filename, minPartitions=(numPartitions or 1))
    rdd2: RDD[Row] = rdd1 \
        .map(parse_line_to_row)
    df = spark_session.spark.createDataFrame(rdd2, SparseLineSchema)
    return df


def rdd_typed_with_index_factory(
        spark_session: TidySparkSession,
        filename: str,
        numPartitions: int | None = None
) -> RDD[LabeledTypedRow]:
    rdd = spark_session.spark_context.textFile(
        filename, minPartitions=(numPartitions or 1))
    rddTypedWithIndex = rdd \
        .map(parse_line_to_types) \
        .zipWithIndex() \
        .map(lambda pair:
             LabeledTypedRow(
                 Index=pair[1],
                 Value=pair[0]))
    return rddTypedWithIndex

# endregion
# region aggregators


def row_to_student_summary(
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


def identify_section_using_intermediate_file(
        src_file_path: str,
) -> str:
    dest_file_path = f"{TEST_DATA_FILE_LOCATION}/temp.csv"
    if os.path.exists(dest_file_path):
        os.unlink(dest_file_path)
    extra_type_pattern = re.compile("^S,")
    section_id = -1
    with open(dest_file_path, "w") as out_fh:
        with open(src_file_path, "r") as in_fh:
            for line in in_fh:
                if extra_type_pattern.match(line):
                    section_id += 1
                assert section_id >= 0
                out_fh.write(f"{section_id},{line}")
    return dest_file_path
# endregion
