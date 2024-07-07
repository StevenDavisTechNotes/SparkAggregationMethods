import os

import pyspark.sql.types as DataTypes
from pyspark import RDD

from challenges.sectional.domain_logic.section_data_parsers import (
    parse_line_to_row, row_to_student_summary)
from challenges.sectional.section_generate_test_data import \
    TEST_DATA_FILE_LOCATION
from challenges.sectional.section_test_data_types import (
    DataSet, SparseLineSchema, StudentSummary, TChallengePythonPysparkAnswer)
from challenges.sectional.using_pyspark.section_pyspark_rdd_prep_shared import \
    section_pyspark_rdd_prep_shared
from utils.tidy_spark_session import TidySparkSession


def section_pyspark_df_prep_grp_csv(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> TChallengePythonPysparkAnswer:
    if data_set.data_size.num_students > pow(10, 8-1):
        # times out
        return "infeasible"
    spark = spark_session.spark
    sectionMaximum = data_set.data.section_maximum
    filename = data_set.data.test_filepath
    SparseLineWithSectionIdLineNoSchema = DataTypes.StructType([
        DataTypes.StructField("SectionId", DataTypes.IntegerType(), True),
        DataTypes.StructField("LineNumber", DataTypes.IntegerType(), True)] +
        SparseLineSchema.fields)

    interFileName = convert_to_row_csv(filename)
    df = spark.read.format("csv") \
        .schema(SparseLineWithSectionIdLineNoSchema) \
        .load(interFileName)
    df = section_pyspark_rdd_prep_shared(df, sectionMaximum)
    rdd: RDD[StudentSummary] = (
        df.rdd
        .map(row_to_student_summary)
        .sortBy(keyfunc=lambda x: x.StudentId)  # pyright: ignore[reportArgumentType]
    )
    return rdd


def convert_to_row_csv(
        srcFilename: str,
) -> str:
    destFilename = f"{TEST_DATA_FILE_LOCATION}/temp.csv"
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
