
from SectionPerfTest.SectionSnippetSubtotal import FIRST_LAST_NEITHER, StudentSnippet2, studentSnippetFromTypedRow2
from SectionPerfTest.SectionTypeDefs import ClassLine, NumDepts, StudentHeader, TrimesterFooter, TrimesterHeader


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
        expected = StudentSnippet2(
            StudentId=studentId,
            StudentName=studentName,
            FirstTrimesterDate=None,
            LastTrimesterDate=None,
            LastMajor=None,
            Credits=[0 for x in range(0, NumDepts)],
            WeightedGradeTotal=[0 for x in range(0, NumDepts)],
            FirstLastFlag=FIRST_LAST_NEITHER,
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = studentSnippetFromTypedRow2(
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
        expected = StudentSnippet2(
            StudentId=None,
            StudentName=None,
            FirstTrimesterDate=date,
            LastTrimesterDate=date,
            LastMajor=None,
            Credits=[0 for x in range(0, NumDepts)],
            WeightedGradeTotal=[0 for x in range(0, NumDepts)],
            FirstLastFlag=FIRST_LAST_NEITHER,
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = studentSnippetFromTypedRow2(
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
        expected = StudentSnippet2(
            StudentId=None,
            StudentName=None,
            FirstTrimesterDate=None,
            LastTrimesterDate=None,
            LastMajor=None,
            Credits=[(credits if x == dept else 0) for x in range(0, NumDepts)],
            WeightedGradeTotal=[(credits * grade if x == dept else 0) for x in range(0, NumDepts)],
            FirstLastFlag=FIRST_LAST_NEITHER,
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = studentSnippetFromTypedRow2(
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
        expected = StudentSnippet2(
            StudentId=None,
            StudentName=None,
            FirstTrimesterDate=None,
            LastTrimesterDate=None,
            LastMajor=2,
            Credits=[0 for x in range(0, NumDepts)],
            WeightedGradeTotal=[0 for x in range(0, NumDepts)],
            FirstLastFlag=FIRST_LAST_NEITHER,
            FirstLineIndex=lineIndex,
            LastLineIndex=lineIndex)
        actual = studentSnippetFromTypedRow2(
            lineIndex=lineIndex,
            rec=src,
        )
        assert expected == actual
