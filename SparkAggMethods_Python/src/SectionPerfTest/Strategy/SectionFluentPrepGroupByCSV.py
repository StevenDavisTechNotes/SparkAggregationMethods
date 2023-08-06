import os
from typing import List, Tuple

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from SectionPerfTest.Strategy.SectionRddPrepShared import section_prep_groupby_core

from Utils.SparkUtils import TidySparkSession

from SectionPerfTest.SectionLogic import parseLineToRow, rowToStudentSummary
from SectionPerfTest.SectionTestData import TEST_DATA_FILE_LOCATION
from SectionPerfTest.SectionTypeDefs import (
    DataSet, SparseLineSchema, StudentSummary)


def section_prepcsv_groupby(
    spark_session: TidySparkSession, data_set: DataSet
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    sectionMaximum = data_set.data.section_maximum
    filename = data_set.data.test_filepath
    SparseLineWithSectionIdLineNoSchema = DataTypes.StructType([
        DataTypes.StructField("SectionId", DataTypes.IntegerType(), True),
        DataTypes.StructField("LineNumber", DataTypes.IntegerType(), True)] +
        SparseLineSchema.fields)

    def convertToRowCsv(srcFilename):
        destFilename = f"{TEST_DATA_FILE_LOCATION}/temp.csv"
        if os.path.exists(destFilename):
            os.unlink(destFilename)
        studentId = None
        with open(destFilename, "w") as outf:
            lineNumber = 0
            with open(srcFilename, "r") as inf:
                for line in inf:
                    lineNumber += 1
                    row = parseLineToRow(line.strip())
                    if row.StudentId is not None:
                        studentId = str(row.StudentId)
                    assert studentId is not None
                    outf.write(",".join([
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

    interFileName = convertToRowCsv(filename)
    df = spark.read.format("csv") \
        .schema(SparseLineWithSectionIdLineNoSchema) \
        .load(interFileName)
    df = section_prep_groupby_core(df, sectionMaximum)
    rdd = df.rdd.map(rowToStudentSummary)
    return None, rdd, None
