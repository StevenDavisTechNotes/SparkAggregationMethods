from pyspark import RDD

from src.challenges.sectional.domain_logic.section_data_parsers import identify_section_using_intermediate_file
from src.challenges.sectional.domain_logic.section_mutable_subtotal_type import aggregate_typed_rows_to_grades
from src.challenges.sectional.section_test_data_types import (
    ClassLine, SectionDataSet, StudentHeader, StudentSummary, TChallengePythonPysparkAnswer, TrimesterFooter,
    TrimesterHeader, TypedLine,
)
from src.utils.tidy_spark_session import TidySparkSession


def section_pyspark_rdd_prep_mappart(
        spark_session: TidySparkSession,
        data_set: SectionDataSet,
) -> TChallengePythonPysparkAnswer:
    if data_set.data_description.num_students > pow(10, 8 - 1):
        # takes too long
        return "infeasible"
    sc = spark_session.spark_context
    filename = data_set.exec_params.source_data_file_path
    target_num_partitions = data_set.exec_params.target_num_partitions

    interFileName = identify_section_using_intermediate_file(filename)
    rdd1: RDD[tuple[tuple[int, int], TypedLine]] = (
        sc.textFile(interFileName, target_num_partitions)
        .zipWithIndex()
        .map(lambda pair: parse_line_to_types_with_line_no(filename, pair[1], pair[0]))
    )
    rdd: RDD[StudentSummary] = (
        rdd1
        .repartitionAndSortWithinPartitions(
            numPartitions=target_num_partitions,
            partitionFunc=lambda x: x[0],  # type: ignore
        )
        .values()
        .mapPartitions(aggregate_typed_rows_to_grades)
        .sortBy(lambda x: x.StudentId)
    )
    return rdd


def parse_line_to_types_with_line_no(
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


def print_test_data_file(
        prepped_file_path: str,
) -> None:
    print(f"Running {__file__}")
    with open(prepped_file_path, "rt") as fh:
        for i_line, line in enumerate(fh):
            parse_line_to_types_with_line_no(prepped_file_path, i_line, line.rstrip())
    print("Done!")
