from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.sectional.domain_logic.section_data_parsers import (
    parse_line_to_types,
)
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    LabeledTypedRow, StudentSummary,
)

from src.challenges.sectional.section_test_data_types_pyspark import (
    SparseLineSchema,
)
from src.utils.tidy_session_pyspark import TidySparkSession

# region parsers


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
