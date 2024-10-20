from typing import Any, Iterable

from src.challenges.sectional.section_pyspark_test_data_types import (
    ClassLine, NumDepartments, StudentHeader, StudentSummary, TrimesterFooter, TrimesterHeader, TypedLine,
)


class MutableTrimester:
    def __init__(self, date: str, wasAbroad: bool):
        self.SourceLines = 1
        self.Credits = [0 for x in range(0, NumDepartments)]
        self.WeightedGradeTotal = [0 for x in range(0, NumDepartments)]
        self.Major = None

    def add_class(self, dept: int, credits: int, grade: int):
        self.SourceLines += 1
        self.Credits[dept] += credits
        self.WeightedGradeTotal[dept] += credits * grade

    def add_footer(self, major: int, gpa: float, credits: int):
        self.SourceLines += 1
        self.Major = major

    def _asdict(self):
        return {"Credits": list(self.Credits), "WGrade": list(
            self.WeightedGradeTotal), "Major": self.Major}


class MutableStudent:
    SourceLines: int
    StudentId: int
    StudentName: str
    LastMajor: int | None
    Credits: list[int]
    WeightedGradeTotal: list[float]

    def __init__(self, studentId: int, studentName: str):
        self.SourceLines = 1
        self.StudentId = studentId
        self.StudentName = studentName
        self.LastMajor = None
        self.Credits = [0 for x in range(0, NumDepartments)]
        self.WeightedGradeTotal = [0 for x in range(0, NumDepartments)]

    def add_trimester(self, trimester: MutableTrimester) -> None:
        self.SourceLines += trimester.SourceLines
        self.LastMajor = trimester.Major
        for dept in range(0, NumDepartments):
            self.Credits[dept] += trimester.Credits[dept]
            self.WeightedGradeTotal[dept] += trimester.WeightedGradeTotal[dept]

    def grade_summary(self) -> StudentSummary:
        assert self.LastMajor is not None
        return StudentSummary(
            StudentId=self.StudentId,
            StudentName=self.StudentName,
            SourceLines=self.SourceLines,
            Major=self.LastMajor,
            GPA=sum(self.WeightedGradeTotal) / max(1, sum(self.Credits)),
            MajorGPA=self.WeightedGradeTotal[self.LastMajor] / max(1, self.Credits[self.LastMajor]),
        )

    def _asdict(self) -> dict[str, Any]:
        return {"StudentId": self.StudentId, "LastMajor": self.LastMajor, "SourceLines": self.SourceLines,
                "Credits": list(self.Credits), "WGrade": list(self.WeightedGradeTotal)}


def aggregate_typed_rows_to_grades(
        iterator: Iterable[TypedLine],
) -> Iterable[StudentSummary]:
    student: MutableStudent | None = None
    trimester: MutableTrimester | None = None
    for lineno, rec in enumerate(iterator):
        match rec:
            case StudentHeader():
                if student is not None:
                    yield student.grade_summary()
                student = MutableStudent(rec.StudentId, rec.StudentName)
            case TrimesterHeader():
                trimester = MutableTrimester(rec.Date, rec.WasAbroad)
            case ClassLine():
                assert trimester is not None
                trimester.add_class(rec.Dept, rec.Credits, rec.Grade)
            case TrimesterFooter():
                assert trimester is not None
                assert student is not None
                trimester.add_footer(rec.Major, rec.GPA, rec.Credits)
                student.add_trimester(trimester)
                trimester = None
            case _:
                raise Exception(
                    f"Unknown parsed row type {rec.__class__.__name__} on line {lineno}")
    if student is not None:
        yield student.grade_summary()
