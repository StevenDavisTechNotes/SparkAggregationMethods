from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    NUM_DEPARTMENTS, ClassLine, StudentHeader, TrimesterFooter, TrimesterHeader,
)

from src.challenges.sectional.domain_logic.section_snippet_subtotal_type import (
    FIRST_LAST_NEITHER, StudentSnippet2, student_snippet_from_typed_row_2,
)


class test_StudentHeader:
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
        expected = StudentSnippet2(
            StudentId=studentId,
            StudentName=studentName,
            FirstTrimesterDate=None,
            LastTrimesterDate=None,
            LastMajor=None,
            Credits=[0 for _ in range(0, NUM_DEPARTMENTS)],
            WeightedGradeTotal=[0 for _ in range(0, NUM_DEPARTMENTS)],
            FirstLastFlag=FIRST_LAST_NEITHER,
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = student_snippet_from_typed_row_2(
            lineIndex=lineIndex,
            rec=src,
        )
        assert expected == actual


class test_TrimesterHeader:
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
        expected = StudentSnippet2(
            StudentId=None,
            StudentName=None,
            FirstTrimesterDate=date,
            LastTrimesterDate=date,
            LastMajor=None,
            Credits=[0 for _ in range(0, NUM_DEPARTMENTS)],
            WeightedGradeTotal=[0 for _ in range(0, NUM_DEPARTMENTS)],
            FirstLastFlag=FIRST_LAST_NEITHER,
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = student_snippet_from_typed_row_2(
            lineIndex=lineIndex,
            rec=src,
        )
        assert expected == actual


class test_ClassLine:
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
        expected = StudentSnippet2(
            StudentId=None,
            StudentName=None,
            FirstTrimesterDate=None,
            LastTrimesterDate=None,
            LastMajor=None,
            Credits=[(credits if x == dept else 0) for x in range(0, NUM_DEPARTMENTS)],
            WeightedGradeTotal=[(credits * grade if x == dept else 0) for x in range(0, NUM_DEPARTMENTS)],
            FirstLastFlag=FIRST_LAST_NEITHER,
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = student_snippet_from_typed_row_2(
            lineIndex=lineIndex,
            rec=src,
        )
        assert expected == actual


class test_TrimesterFooter:
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
        expected = StudentSnippet2(
            StudentId=None,
            StudentName=None,
            FirstTrimesterDate=None,
            LastTrimesterDate=None,
            LastMajor=2,
            Credits=[0 for _ in range(0, NUM_DEPARTMENTS)],
            WeightedGradeTotal=[0 for _ in range(0, NUM_DEPARTMENTS)],
            FirstLastFlag=FIRST_LAST_NEITHER,
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = student_snippet_from_typed_row_2(
            lineIndex=lineIndex,
            rec=src,
        )
        assert expected == actual
