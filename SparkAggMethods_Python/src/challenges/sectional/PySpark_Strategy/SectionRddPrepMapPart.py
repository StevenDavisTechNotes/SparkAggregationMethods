from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from challenges.sectional.SectionLogic import \
    identifySectionUsingIntermediateFile
from challenges.sectional.SectionMutuableSubtotal import \
    aggregateTypedRowsToGrades
from challenges.sectional.SectionTypeDefs import (ClassLine, DataSet,
                                                  StudentHeader,
                                                  StudentSummary,
                                                  TrimesterFooter,
                                                  TrimesterHeader, TypedLine)
from utils.TidySparkSession import TidySparkSession


def section_prep_mappart(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> tuple[list[StudentSummary] | None, RDD[StudentSummary] | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    filename = data_set.data.test_filepath
    TargetNumPartitions = data_set.data.target_num_partitions

    interFileName = identifySectionUsingIntermediateFile(filename)
    rdd1: RDD[tuple[tuple[int, int], TypedLine]] = (
        sc.textFile(interFileName, TargetNumPartitions)
        .zipWithIndex()
        .map(lambda pair: parseLineToTypesWithLineNo(filename, pair[1], pair[0]))
    )
    rdd: RDD[StudentSummary] = (
        rdd1
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0],  # type: ignore
        )
        .values()
        .mapPartitions(aggregateTypedRowsToGrades)
        .sortBy(lambda x: x.StudentId)
    )
    return None, rdd, None


def parseLineToTypesWithLineNo(
        filename: str,
        lineNumber: int,
        line: str,
) -> tuple[tuple[int, int], TypedLine]:
    str_section_id, row_type, *fields = line.split(',')
    section_id = int(str_section_id)
    match row_type:
        case 'S':
            str_student_id, student_name = fields
            student_id = int(str_student_id)
            return (
                (section_id, lineNumber),
                StudentHeader(
                    StudentId=student_id, StudentName=student_name)
            )
        case 'TH':
            str_date, str_was_abroad = fields
            was_abroad = str_was_abroad == 'True'
            return (
                (section_id, lineNumber),
                TrimesterHeader(
                    Date=str_date, WasAbroad=was_abroad)
            )
        case 'C':
            str_dept, str_credits, str_grade = fields
            dept = int(str_dept)
            credits = int(str_credits)
            grade = int(str_grade)
            return (
                (section_id, lineNumber),
                ClassLine(
                    Dept=dept, Credits=credits, Grade=grade)
            )
        case 'TF':
            str_major, str_gpa, str_credits = fields
            major = int(str_major)
            gpa = float(str_gpa)
            credits = int(str_credits)
            return (
                (section_id, lineNumber),
                TrimesterFooter(
                    Major=major, GPA=gpa, Credits=credits)
            )
    raise Exception(
        f"Unknown parsed row type {row_type} on line {lineNumber} in file {filename}")


if __name__ == "__main__":
    prepped_file_path = "D:\\temp\\SparkPerfTesting\\temp.csv"
    with open(prepped_file_path, "rt") as fh:
        for iline, line in enumerate(fh):
            parseLineToTypesWithLineNo(prepped_file_path, iline, line.rstrip())
