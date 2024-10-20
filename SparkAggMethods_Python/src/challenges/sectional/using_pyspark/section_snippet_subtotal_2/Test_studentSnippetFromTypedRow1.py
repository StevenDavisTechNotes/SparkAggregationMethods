
from src.challenges.sectional.domain_logic.section_snippet_subtotal_type import (
    StudentSnippet1, student_snippet_from_typed_row_1,
)
from src.challenges.sectional.section_pyspark_test_data_types import (
    ClassLine, NumDepartments, StudentHeader, TrimesterFooter, TrimesterHeader,
)


class Test_StudentHeader:
    def test_nominal(
            self,
    ) -> None:
        studentId = 123
        studentName = 'xxx'
        lineIndex = 456
        src = StudentHeader(
            StudentId=studentId,
            StudentName=studentName,
        )
        expected = StudentSnippet1(
            StudentId=studentId,
            StudentName=studentName,
            FirstTrimesterDate=None,
            LastTrimesterDate=None,
            LastMajor=None,
            Credits=[0 for _ in range(0, NumDepartments)],
            WeightedGradeTotal=[0 for _ in range(0, NumDepartments)],
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = student_snippet_from_typed_row_1(
            lineIndex=lineIndex,
            rec=src,
        )
        assert expected == actual


class Test_TrimesterHeader:
    def test_nominal(
            self,
    ) -> None:
        date = 'date'
        wasAbroad = False
        lineIndex = 456
        src = TrimesterHeader(
            Date=date,
            WasAbroad=wasAbroad,
        )
        expected = StudentSnippet1(
            StudentId=None,
            StudentName=None,
            FirstTrimesterDate=date,
            LastTrimesterDate=date,
            LastMajor=None,
            Credits=[0 for _ in range(0, NumDepartments)],
            WeightedGradeTotal=[0 for _ in range(0, NumDepartments)],
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = student_snippet_from_typed_row_1(
            lineIndex=lineIndex,
            rec=src,
        )
        assert expected == actual


class Test_ClassLine:
    def test_nominal(
            self,
    ) -> None:
        dept = 2
        credits = 3
        grade = 5
        lineIndex = 456
        src = ClassLine(
            Dept=dept,
            Credits=credits,
            Grade=grade,
        )
        expected = StudentSnippet1(
            StudentId=None,
            StudentName=None,
            FirstTrimesterDate=None,
            LastTrimesterDate=None,
            LastMajor=None,
            Credits=[(credits if x == dept else 0) for x in range(0, NumDepartments)],
            WeightedGradeTotal=[(credits * grade if x == dept else 0) for x in range(0, NumDepartments)],
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = student_snippet_from_typed_row_1(
            lineIndex=lineIndex,
            rec=src,
        )
        assert expected == actual


class Test_TrimesterFooter:
    def test_nominal(
            self,
    ) -> None:
        major = 2
        gps = 3.2
        credits = 21
        lineIndex = 456
        src = TrimesterFooter(
            Major=major,
            GPA=gps,
            Credits=credits,
        )
        expected = StudentSnippet1(
            StudentId=None,
            StudentName=None,
            FirstTrimesterDate=None,
            LastTrimesterDate=None,
            LastMajor=2,
            Credits=[0 for _ in range(0, NumDepartments)],
            WeightedGradeTotal=[0 for _ in range(0, NumDepartments)],
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = student_snippet_from_typed_row_1(
            lineIndex=lineIndex,
            rec=src,
        )
        assert expected == actual
