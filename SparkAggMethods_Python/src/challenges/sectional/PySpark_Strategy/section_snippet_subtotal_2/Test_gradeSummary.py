from challenges.sectional.SectionSnippetSubtotal import (CompletedStudent,
                                                         gradeSummary)
from challenges.sectional.SectionTypeDefs import NumDepts, StudentSummary
from utils.PrintObjectToFile import PrintObjectAsPythonLiteral


def test_gpa_math():
    studentId = 123
    studentName = 'xxx'
    credits = 3
    major = 2
    credits = [(21 if x == major else 11) for x in range(NumDepts)]
    weightedGradeTotal = [
        round(
            (4.73 if x == major else 3.11)
            * credits[x])
        for x in range(NumDepts)]
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
    actual = gradeSummary(student)
    PrintObjectAsPythonLiteral(actual)
    assert expected == actual
