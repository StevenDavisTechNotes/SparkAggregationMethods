from typing import Any, cast

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.sectional.domain_logic.sectional_data_parsers import (
    identify_section_using_intermediate_file,
)
from spark_agg_methods_common_python.challenges.sectional.sectional_test_data_types import StudentSummary
from src.challenges.sectional.domain_logic.sectional_data_parsers_pyspark import row_to_student_summary
from src.challenges.sectional.sectional_test_data_types_pyspark import (
    MAX_PARTITIONS_PER_STAGE_SECTIONAL_PYSPARK, SectionalDataSetPyspark, SectionalExecutionParametersPyspark,
    SparseLineSchema, TChallengePythonPysparkAnswer,
)
from src.challenges.sectional.strategies.using_pyspark.sectional_pyspark_rdd_prep_shared import (
    sectional_pyspark_rdd_prep_shared,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def student_id_accessor(x: StudentSummary) -> Any:
    return x.StudentId


def sectional_pyspark_df_prep_txt(
        spark_session: TidySparkSession,
        exec_params: SectionalExecutionParametersPyspark,
        data_set: SectionalDataSetPyspark,
) -> TChallengePythonPysparkAnswer:
    if data_set.target_num_partitions > MAX_PARTITIONS_PER_STAGE_SECTIONAL_PYSPARK:
        return "infeasible", "Too many partitions: one Python worker (and port) per task exhausts Windows ports"
    sc = spark_session.spark_context
    spark = spark_session.spark
    section_maximum = data_set.section_maximum
    filename = data_set.source_data_file_path

    SparseLineWithSectionIdLineNoSchema = DataTypes.StructType([
        DataTypes.StructField("SectionId", DataTypes.IntegerType(), True),
        DataTypes.StructField("LineNumber", DataTypes.IntegerType(), True)] +
        SparseLineSchema.fields)

    interFileName = identify_section_using_intermediate_file(filename)
    rdd = (
        sc.textFile(str(interFileName), data_set.target_num_partitions)
        .zipWithIndex()
        .map(parse_line_to_row_with_line_no)
    )
    df = spark.createDataFrame(rdd, SparseLineWithSectionIdLineNoSchema)
    df = sectional_pyspark_rdd_prep_shared(
        dfSrc=df,
        exec_params=exec_params,
        sectionMaximum=section_maximum,
    )
    rdd = (
        cast(RDD[Row], df.rdd)
        .map(row_to_student_summary)
        .sortBy(keyfunc=student_id_accessor)
    )
    return rdd


def parse_line_to_row_with_line_no(
        arg: tuple[str, int],
) -> Row:
    lineNumber = int(arg[1])
    line = arg[0]
    fields = line.split(",")
    sectionId = int(fields[0])
    fields = fields[1:]
    lineType = fields[0]
    if lineType == "S":
        return Row(
            SectionId=sectionId, LineNumber=lineNumber,
            Type=fields[0],
            StudentId=int(fields[1]), StudentName=fields[2],
            Date=None, WasAbroad=None,
            Dept=None, ClassCredits=None, ClassGrade=None,
            Major=None, TriGPA=None, TriCredits=None)
    if lineType == "TH":
        return Row(
            SectionId=sectionId, LineNumber=lineNumber,
            Type=fields[0],
            StudentId=None, StudentName=None,
            Date=fields[1], WasAbroad=(fields[2] == "True"),
            Dept=None, ClassCredits=None, ClassGrade=None,
            Major=None, TriGPA=None, TriCredits=None)
    if lineType == "C":
        return Row(
            SectionId=sectionId, LineNumber=lineNumber,
            Type=fields[0],
            StudentId=None, StudentName=None,
            Date=None, WasAbroad=None,
            Dept=int(fields[1]), ClassCredits=int(fields[2]), ClassGrade=int(fields[3]),
            Major=None, TriGPA=None, TriCredits=None)
    if lineType == "TF":
        return Row(
            SectionId=sectionId, LineNumber=lineNumber,
            Type=fields[0],
            StudentId=None, StudentName=None,
            Date=None, WasAbroad=None,
            Dept=None, ClassCredits=None, ClassGrade=None,
            Major=int(fields[1]), TriGPA=float(fields[2]), TriCredits=int(fields[3]))
    raise ValueError("Malformed data " + line)
