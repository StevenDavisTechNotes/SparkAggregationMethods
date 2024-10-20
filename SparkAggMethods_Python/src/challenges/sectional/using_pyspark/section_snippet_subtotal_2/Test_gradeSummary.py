from spark_agg_methods_common_python.utils.printer import print_object_as_python_literal

from src.challenges.sectional.domain_logic.section_snippet_subtotal_type import CompletedStudent, grade_summary
from src.challenges.sectional.section_pyspark_test_data_types import NumDepartments, StudentSummary


def test_gpa_math():
    studentId = 123
    studentName = 'xxx'
    credits = 3
    major = 2
    credits = [(21 if x == major else 11) for x in range(NumDepartments)]
    weightedGradeTotal = [
        round(
            (4.73 if x == major else 3.11)
            * credits[x])
        for x in range(NumDepartments)]
    firstLineIndex = 456
    lastLineIndex = 496
    student = CompletedStudent(
        StudentId=studentId,
        StudentName=studentName,
        LastMajor=major,
        Credits=credits,
        WeightedGradeTotal=weightedGradeTotal,
        FirstLineIndex=firstLineIndex,
        LastLineIndex=lastLineIndex,
    )
    expected = StudentSummary(
        StudentId=123,
        StudentName='xxx',
        SourceLines=41,
        GPA=3.7222222222222223,
        Major=2,
        MajorGPA=4.714285714285714
    )
    actual = grade_summary(student)
    print_object_as_python_literal(actual)
    assert expected == actual
