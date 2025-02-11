from typing import NamedTuple

from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    NUM_DEPARTMENTS, ClassLine, StudentHeader, StudentSummary, TrimesterFooter,
    TrimesterHeader, TypedLine,
)

FIRST_LAST_FIRST = 1
FIRST_LAST_LAST = -1
FIRST_LAST_NEITHER = 0


class StudentSnippet1(NamedTuple):
    StudentId: int | None
    StudentName: str | None
    FirstTrimesterDate: str | None
    LastTrimesterDate: str | None
    LastMajor: int | None
    Credits: list[int]
    WeightedGradeTotal: list[int]
    FirstLineIndex: int
    LastLineIndex: int


class StudentSnippet2(NamedTuple):
    FirstLastFlag: int
    FirstLineIndex: int
    LastLineIndex: int
    StudentId: int | None = None
    StudentName: str | None = None
    FirstTrimesterDate: str | None = None
    LastTrimesterDate: str | None = None
    LastMajor: int | None = None
    Credits: list[int] | None = None
    WeightedGradeTotal: list[int] | None = None


class CompletedStudent(NamedTuple):
    StudentId: int
    StudentName: str
    LastMajor: int
    Credits: list[int]
    WeightedGradeTotal: list[int]
    FirstLineIndex: int
    LastLineIndex: int


def student_snippet_from_typed_row_1(
    lineIndex: int,
    rec: TypedLine,
) -> StudentSnippet1:
    credits = [0 for x in range(0, NUM_DEPARTMENTS)]
    weightedGradeTotal = [0 for x in range(0, NUM_DEPARTMENTS)]
    match rec:
        case StudentHeader():
            return StudentSnippet1(
                StudentId=rec.StudentId,
                StudentName=rec.StudentName,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=credits,
                WeightedGradeTotal=weightedGradeTotal,
                FirstLineIndex=lineIndex,
                LastLineIndex=lineIndex)
        case TrimesterHeader():
            return StudentSnippet1(
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=rec.Date,
                LastTrimesterDate=rec.Date,
                LastMajor=None,
                Credits=credits,
                WeightedGradeTotal=weightedGradeTotal,
                FirstLineIndex=lineIndex,
                LastLineIndex=lineIndex)
        case ClassLine():
            credits[rec.Dept] += rec.Credits
            weightedGradeTotal[rec.Dept] += rec.Credits * rec.Grade
            return StudentSnippet1(
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=credits,
                WeightedGradeTotal=weightedGradeTotal,
                FirstLineIndex=lineIndex,
                LastLineIndex=lineIndex)
        case TrimesterFooter():
            return StudentSnippet1(
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=rec.Major,
                Credits=credits,
                WeightedGradeTotal=weightedGradeTotal,
                FirstLineIndex=lineIndex,
                LastLineIndex=lineIndex)
        case _:
            raise Exception("Unknown parsed row type")


def student_snippet_from_typed_row_2(
    lineIndex: int,
    rec: TypedLine,
) -> StudentSnippet2:
    credits = [0 for x in range(0, NUM_DEPARTMENTS)]
    weightedGradeTotal = [0 for x in range(0, NUM_DEPARTMENTS)]
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
                StudentId=None,
                StudentName=None,
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


def completed_from_snippet_1(
        lhs: StudentSnippet1,
) -> CompletedStudent:
    assert lhs.StudentId is not None
    assert lhs.StudentName is not None
    assert lhs.LastMajor is not None
    return CompletedStudent(
        StudentId=lhs.StudentId,
        StudentName=lhs.StudentName,
        LastMajor=lhs.LastMajor,
        Credits=lhs.Credits,
        WeightedGradeTotal=lhs.WeightedGradeTotal,
        FirstLineIndex=lhs.FirstLineIndex,
        LastLineIndex=lhs.LastLineIndex)


def completed_from_snippet_2(
        lhs: StudentSnippet2,
) -> CompletedStudent:
    assert lhs.StudentId is not None
    assert lhs.StudentName is not None
    assert lhs.LastMajor is not None
    assert lhs.Credits is not None
    assert lhs.WeightedGradeTotal is not None
    return CompletedStudent(
        StudentId=lhs.StudentId,
        StudentName=lhs.StudentName,
        LastMajor=lhs.LastMajor,
        Credits=lhs.Credits,
        WeightedGradeTotal=lhs.WeightedGradeTotal,
        FirstLineIndex=lhs.FirstLineIndex,
        LastLineIndex=lhs.LastLineIndex)


def marge_snippets_2(
        lhs: StudentSnippet2,
        rhs: StudentSnippet2
) -> StudentSnippet2:
    assert lhs.LastLineIndex + 1 == rhs.FirstLineIndex
    credits = [
        (0 if lhs.Credits is None else lhs.Credits[dept])
        + (0 if rhs.Credits is None else rhs.Credits[dept])
        for dept in range(0, NUM_DEPARTMENTS)
    ]
    weightedGradeTotal = [
        (0 if lhs.WeightedGradeTotal is None else lhs.WeightedGradeTotal[dept])
        + (0 if rhs.WeightedGradeTotal is None else rhs.WeightedGradeTotal[dept])
        for dept in range(0, NUM_DEPARTMENTS)
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


def complete_snippets_2(
        building_snippet: StudentSnippet2,
        front_is_clean: bool,
        back_is_clean: bool,
) -> tuple[list[CompletedStudent], list[StudentSnippet2]]:
    if front_is_clean and back_is_clean:
        return [completed_from_snippet_2(building_snippet)], []
    return [], [building_snippet]


def merge_snippet_lists_1(
        lh_group: list[StudentSnippet1],
        rh_group: list[StudentSnippet1]
) -> list[StudentSnippet1]:
    if len(lh_group) == 0:
        return rh_group
    for rhs in rh_group:
        lhs = lh_group[-1]
        # if done with the last student, start the next one
        if rhs.StudentId is not None:
            lh_group.append(rhs)
            continue
        # else combine rhs to the previous snippet
        assert lhs.LastLineIndex + 1 == rhs.FirstLineIndex
        lh_group[-1] = StudentSnippet1(
            StudentId=lhs.StudentId,
            StudentName=lhs.StudentName,
            FirstTrimesterDate=lhs.FirstTrimesterDate if lhs.FirstTrimesterDate is not None else rhs.FirstTrimesterDate,
            LastTrimesterDate=(
                rhs.LastTrimesterDate
                if rhs.LastTrimesterDate is not None
                else lhs.LastTrimesterDate),
            LastMajor=rhs.LastMajor,
            Credits=[
                lhs.Credits[dept] + rhs.Credits[dept]
                for dept in range(0, NUM_DEPARTMENTS)],
            WeightedGradeTotal=[
                lhs.WeightedGradeTotal[dept] + rhs.WeightedGradeTotal[dept]
                for dept in range(0, NUM_DEPARTMENTS)],
            FirstLineIndex=lhs.FirstLineIndex,
            LastLineIndex=rhs.LastLineIndex)
    return lh_group


def grade_summary(
        x: CompletedStudent
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
