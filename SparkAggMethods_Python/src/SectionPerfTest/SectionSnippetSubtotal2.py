from typing import List, NamedTuple, Optional

from SectionPerfTest.SectionTypeDefs import (
    ClassLine, NumDepts, StudentHeader, StudentSummary, TrimesterFooter, TrimesterHeader, TypedLine)

FIRST_LAST_FIRST = 1
FIRST_LAST_LAST = -1
FIRST_LAST_NEITHER = 0


class StudentSnippet2(NamedTuple):
    FirstLastFlag: int
    FirstLineIndex: int
    LastLineIndex: int
    StudentId: Optional[int] = None
    StudentName: Optional[str] = None
    FirstTrimesterDate: Optional[str] = None
    LastTrimesterDate: Optional[str] = None
    LastMajor: Optional[int] = None
    Credits: Optional[List[int]] = None
    WeightedGradeTotal: Optional[List[int]] = None


class CompletedStudent2(NamedTuple):
    StudentId: int
    StudentName: str
    LastMajor: int
    Credits: List[int]
    WeightedGradeTotal: List[int]
    FirstLineIndex: int
    LastLineIndex: int


def studentSnippetFromTypedRow2(
    lineIndex: int,
    rec: TypedLine,
) -> StudentSnippet2:
    credits = [0 for x in range(0, NumDepts)]
    weightedGradeTotal = [0 for x in range(0, NumDepts)]
    match rec:
        case StudentHeader():
            return StudentSnippet2(
                StudentId=rec.StudentId,
                StudentName=rec.StudentName,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=credits,
                WeightedGradeTotal=weightedGradeTotal,
                FirstLastFlag=FIRST_LAST_NEITHER,
                FirstLineIndex=lineIndex,
                LastLineIndex=lineIndex)
        case TrimesterHeader():
            return StudentSnippet2(
                StudentId=None, StudentName=None,
                FirstTrimesterDate=rec.Date,
                LastTrimesterDate=rec.Date,
                LastMajor=None,
                Credits=credits,
                WeightedGradeTotal=weightedGradeTotal,
                FirstLastFlag=FIRST_LAST_NEITHER,
                FirstLineIndex=lineIndex,
                LastLineIndex=lineIndex)
        case ClassLine():
            credits[rec.Dept] += rec.Credits
            weightedGradeTotal[rec.Dept] += rec.Credits * rec.Grade
            return StudentSnippet2(
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=credits,
                WeightedGradeTotal=weightedGradeTotal,
                FirstLastFlag=FIRST_LAST_NEITHER,
                FirstLineIndex=lineIndex,
                LastLineIndex=lineIndex)
        case TrimesterFooter():
            return StudentSnippet2(
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=rec.Major,
                Credits=credits,
                WeightedGradeTotal=weightedGradeTotal,
                FirstLastFlag=FIRST_LAST_NEITHER,
                FirstLineIndex=lineIndex,
                LastLineIndex=lineIndex)
        case _:
            raise Exception("Unknown parsed row type")


def completedFromSnippet2(
        lhs: StudentSnippet2,
) -> CompletedStudent2:
    assert lhs.StudentId is not None
    assert lhs.StudentName is not None
    assert lhs.LastMajor is not None
    assert lhs.Credits is not None
    assert lhs.WeightedGradeTotal is not None
    return CompletedStudent2(
        StudentId=lhs.StudentId,
        StudentName=lhs.StudentName,
        LastMajor=lhs.LastMajor,
        Credits=lhs.Credits,
        WeightedGradeTotal=lhs.WeightedGradeTotal,
        FirstLineIndex=lhs.FirstLineIndex,
        LastLineIndex=lhs.LastLineIndex)


def margeSnippets2(
        lhs: StudentSnippet2,
        rhs: StudentSnippet2
) -> StudentSnippet2:
    if lhs.LastLineIndex + 1 != rhs.FirstLineIndex:
        print('about to assert ',
              lhs.LastLineIndex, rhs.FirstLineIndex)
    assert lhs.LastLineIndex + 1 == rhs.FirstLineIndex
    credits = [
        (0 if lhs.Credits is None else lhs.Credits[dept])
        + (0 if rhs.Credits is None else rhs.Credits[dept])
        for dept in range(0, NumDepts)
    ]
    weightedGradeTotal = [
        (0 if lhs.WeightedGradeTotal is None else lhs.WeightedGradeTotal[dept])
        + (0 if rhs.WeightedGradeTotal is None else rhs.WeightedGradeTotal[dept])
        for dept in range(0, NumDepts)
    ]
    return StudentSnippet2(
        StudentId=lhs.StudentId,
        StudentName=lhs.StudentName,
        FirstTrimesterDate=(
            lhs.FirstTrimesterDate
            if lhs.FirstTrimesterDate is not None
            else rhs.FirstTrimesterDate),
        LastTrimesterDate=(
            rhs.LastTrimesterDate
            if rhs.LastTrimesterDate is not None
            else lhs.LastTrimesterDate),
        LastMajor=rhs.LastMajor,
        Credits=credits,
        WeightedGradeTotal=weightedGradeTotal,
        FirstLastFlag=FIRST_LAST_NEITHER,
        FirstLineIndex=lhs.FirstLineIndex,
        LastLineIndex=rhs.LastLineIndex)


def completeSnippets2(
        building_snippet: StudentSnippet2,
        front_is_clean: bool,
        back_is_clean: bool,
) -> tuple[List[CompletedStudent2], List[StudentSnippet2]]:
    # assert front_is_clean == (building_snippet.StudentId is not None)
    if front_is_clean and back_is_clean:
        return [completedFromSnippet2(building_snippet)], []
    return [], [building_snippet]


def gradeSummary2(
        x: CompletedStudent2
) -> StudentSummary:
    assert x.LastMajor is not None
    return StudentSummary(
        StudentId=x.StudentId,
        StudentName=x.StudentName,
        SourceLines=x.LastLineIndex - x.FirstLineIndex + 1,
        Major=x.LastMajor,
        GPA=sum(x.WeightedGradeTotal) / max(1, sum(x.Credits)),
        MajorGPA=x.WeightedGradeTotal[x.LastMajor] /
        max(1, x.Credits[x.LastMajor])
    )
