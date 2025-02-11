import os

import pyspark.sql.types as DataTypes
from pyspark import RDD
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    StudentSummary,
)
from spark_agg_methods_common_python.perf_test_common import (
    LOCAL_TEST_DATA_FILE_LOCATION,
)

from src.challenges.sectional.domain_logic.section_data_parsers_pyspark import (
    parse_line_to_row, row_to_student_summary,
)
from src.challenges.sectional.section_test_data_types_pyspark import (
    SectionDataSetPyspark, SectionExecutionParametersPyspark, SparseLineSchema,
    TChallengePythonPysparkAnswer,
)
from src.challenges.sectional.strategies.using_pyspark.section_pyspark_rdd_prep_shared import (
    section_pyspark_rdd_prep_shared,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def section_pyspark_df_prep_grp_csv(
        spark_session: TidySparkSession,
        exec_params: SectionExecutionParametersPyspark,
        data_set: SectionDataSetPyspark,
) -> TChallengePythonPysparkAnswer:
    if data_set.data_description.num_students > pow(10, 8-1):
        return "infeasible", "Times out"
    spark = spark_session.spark
    section_maximum = data_set.section_maximum
    filename = data_set.source_data_file_path
    SparseLineWithSectionIdLineNoSchema = DataTypes.StructType([
        DataTypes.StructField("SectionId", DataTypes.IntegerType(), True),
        DataTypes.StructField("LineNumber", DataTypes.IntegerType(), True)] +
        SparseLineSchema.fields)

    intermediate_file_nameFileName = convert_to_row_csv(filename)
    df = spark.read.format("csv") \
        .schema(SparseLineWithSectionIdLineNoSchema) \
        .load(intermediate_file_nameFileName)
    df = section_pyspark_rdd_prep_shared(df, exec_params, section_maximum)
    rdd: RDD[StudentSummary] = (
        df.rdd
        .map(row_to_student_summary)
        .sortBy(keyfunc=lambda x: x.StudentId)  # pyright: ignore[reportArgumentType]
    )
    return rdd


def convert_to_row_csv(
        srcFilename: str,
) -> str:
    destFilename = f"{LOCAL_TEST_DATA_FILE_LOCATION}/temp.csv"
    if os.path.exists(destFilename):
        os.unlink(destFilename)
    studentId = None
    with open(destFilename, "w") as out_fh:
        lineNumber = 0
        with open(srcFilename, "r") as in_fh:
            for line in in_fh:
                lineNumber += 1
                row = parse_line_to_row(line.strip())
                if row.StudentId is not None:
                    studentId = str(row.StudentId)
                assert studentId is not None
                out_fh.write(",".join([
                    studentId, str(lineNumber), row.Type,
                    str(row.StudentId) if row.StudentId is not None else '',
                    row.StudentName if row.StudentName is not None else '',
                    row.Date if row.Date is not None else '',
                    str(row.WasAbroad) if row.WasAbroad is not None else '',
                    str(row.Dept) if row.Dept is not None else '',
                    str(row.ClassCredits) if row.ClassCredits is not None else '',
                    str(row.ClassGrade) if row.ClassGrade is not None else '',
                    str(row.Major) if row.Major is not None else '',
                    str(row.TriGPA) if row.TriGPA is not None else '',
                    str(row.TriCredits) if row.TriCredits is not None else '',
                    "\n"
                ]))
    return destFilename
