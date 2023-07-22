import collections
import os
import re
from typing import Any, Dict, Iterable

from pyspark import RDD
from pyspark.sql import Row

from Utils.SparkUtils import TidySparkSession
from .SectionTestData import TEST_DATA_FILE_LOCATION
from .SectionTypeDefs import (
    ClassLine,
    LabeledTypedRow, NumDepts, SparseLineSchema,
    StudentHeader, StudentSummary, TrimesterFooter,
    TrimesterHeader,
)

# region parsers


def parseLineToTypes(line):
    fields = line.rstrip().split(',')
    if fields[0] == 'S':
        return StudentHeader(StudentId=int(fields[1]), StudentName=fields[2])
    if fields[0] == 'TH':
        return TrimesterHeader(Date=fields[1], WasAbroad=(fields[2] == 'True'))
    if fields[0] == 'C':
        return ClassLine(Dept=int(fields[1]), Credits=int(
            fields[2]), Grade=int(fields[3]))
    if fields[0] == 'TF':
        return TrimesterFooter(Major=int(fields[1]), GPA=float(
            fields[2]), Credits=int(fields[3]))
    raise Exception("Malformed data " + line)


def parseLineToRow(line):
    fields = line.split(',')
    if fields[0] == 'S':
        return Row(Type=fields[0],
                   StudentId=int(fields[1]), StudentName=fields[2],
                   Date=None, WasAbroad=None,
                   Dept=None, ClassCredits=None, ClassGrade=None,
                   Major=None, TriGPA=None, TriCredits=None)
    if fields[0] == 'TH':
        return Row(Type=fields[0],
                   StudentId=None, StudentName=None,
                   Date=fields[1], WasAbroad=(fields[2] == 'True'),
                   Dept=None, ClassCredits=None, ClassGrade=None,
                   Major=None, TriGPA=None, TriCredits=None)
    if fields[0] == 'C':
        return Row(Type=fields[0],
                   StudentId=None, StudentName=None,
                   Date=None, WasAbroad=None,
                   Dept=int(fields[1]), ClassCredits=int(fields[2]), ClassGrade=int(fields[3]),
                   Major=None, TriGPA=None, TriCredits=None)
    if fields[0] == 'TF':
        return Row(Type=fields[0],
                   StudentId=None, StudentName=None,
                   Date=None, WasAbroad=None,
                   Dept=None, ClassCredits=None, ClassGrade=None,
                   Major=int(fields[1]), TriGPA=float(fields[2]), TriCredits=int(fields[3]))
    raise Exception("Malformed data " + line)


def dfSparseRowsFactory(spark_session: TidySparkSession,
                        filename: str, numPartitions: int | None = None):
    rdd = spark_session.spark_context.textFile(
        filename, minPartitions=(numPartitions or 1))
    rdd = rdd \
        .map(parseLineToRow)
    df = spark_session.spark.createDataFrame(rdd, SparseLineSchema)
    return df


def rddTypedWithIndexFactory(
        spark_session: TidySparkSession, filename: str, numPartitions: int | None = None
) -> RDD[LabeledTypedRow]:
    rdd = spark_session.spark_context.textFile(
        filename, minPartitions=(numPartitions or 1))
    rddTypedWithIndex = rdd \
        .map(parseLineToTypes) \
        .zipWithIndex() \
        .map(lambda pair:
             LabeledTypedRow(
                 Index=pair[1],
                 Value=pair[0]))
    return rddTypedWithIndex

# endregion
# region Mutable


class MutableTrimester:
    def __init__(self, date, wasAbroad):
        self.SourceLines = 1
        self.Credits = [0 for x in range(0, NumDepts)]
        self.WeightedGradeTotal = [0 for x in range(0, NumDepts)]
        self.Major = None

    def addClass(self, dept, credits, grade):
        self.SourceLines += 1
        self.Credits[dept] += credits
        self.WeightedGradeTotal[dept] += credits * grade

    def addFooter(self, major, gpa, credits):
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

    def __init__(self, studentId, studentName):
        self.SourceLines = 1
        self.StudentId = studentId
        self.StudentName = studentName
        self.LastMajor = None
        self.Credits = [0 for x in range(0, NumDepts)]
        self.WeightedGradeTotal = [0 for x in range(0, NumDepts)]

    def addTrimester(self, trimester: MutableTrimester) -> None:
        self.SourceLines += trimester.SourceLines
        self.LastMajor = trimester.Major
        for dept in range(0, NumDepts):
            self.Credits[dept] += trimester.Credits[dept]
            self.WeightedGradeTotal[dept] += trimester.WeightedGradeTotal[dept]

    def gradeSummary(self) -> StudentSummary:
        return StudentSummary(
            StudentId=self.StudentId,
            StudentName=self.StudentName,
            SourceLines=self.SourceLines,
            Major=self.LastMajor,
            GPA=sum(self.WeightedGradeTotal) / max(1, sum(self.Credits)),
            MajorGPA=self.WeightedGradeTotal[self.LastMajor] /
            max(1, self.Credits[self.LastMajor])
            if self.LastMajor is not None else None
        )

    # def gradeSummaryRow(self):
    #     return Row(**self.gradeSummary())

    def _asdict(self) -> Dict[str, Any]:
        return {"StudentId": self.StudentId, "LastMajor": self.LastMajor, "SourceLines": self.SourceLines,
                "Credits": list(self.Credits), "WGrade": list(self.WeightedGradeTotal)}
# endregion
# region aggregators


def aggregateTypedRowsToGrades(iterator) -> Iterable[StudentSummary]:
    student: MutableStudent | None = None
    trimester: MutableTrimester | None = None
    for lineno, rec in enumerate(iterator):
        match rec:
            case StudentHeader():
                if student is not None:
                    yield student.gradeSummary()
                student = MutableStudent(rec.StudentId, rec.StudentName)
            case TrimesterHeader():
                trimester = MutableTrimester(rec.Date, rec.WasAbroad)
            case ClassLine():
                assert trimester is not None
                trimester.addClass(rec.Dept, rec.Credits, rec.Grade)
            case TrimesterFooter():
                assert trimester is not None
                assert student is not None
                trimester.addFooter(rec.Major, rec.GPA, rec.Credits)
                student.addTrimester(trimester)
                trimester = None
            case _:
                raise Exception(
                    f"Unknown parsed row type {rec.__class__.__name__} on line {lineno}")
    if student is not None:
        yield student.gradeSummary()



def rowToStudentSummary(x):
    return StudentSummary(
        StudentId=x.StudentId,
        StudentName=x.StudentName,
        SourceLines=x.SourceLines,
        Major=x.Major,
        GPA=x.GPA,
        MajorGPA=x.MajorGPA)



# endregion
# region Snippets
StudentSnippet = collections.namedtuple("StudentSnippet",
                                        ["StudentId", "StudentName",
                                         "FirstTrimester", "LastTrimester", "LastMajor", "Credits", "WeightedGradeTotal",
                                         "FirstLineIndex", "LastLineIndex"])
CompletedStudent = collections.namedtuple("CompletedStudent",
                                          ["StudentId", "StudentName", "SourceLines", "LastMajor", "Credits", "WeightedGradeTotal",
                                           "FirstLineIndex", "LastLineIndex"])


class StudentSnippetBuilder:
    @staticmethod
    def studentSnippetFromTypedRow(lineIndex, rec):
        credits = [0 for x in range(0, NumDepts)]
        weightedGradeTotal = [0 for x in range(0, NumDepts)]
        match rec:
            case StudentHeader():
                return StudentSnippet(
                    StudentId=rec.StudentId, StudentName=rec.StudentName,
                    FirstTrimester=None, LastTrimester=None,
                    LastMajor=None,
                    Credits=credits, WeightedGradeTotal=weightedGradeTotal,
                    FirstLineIndex=lineIndex, LastLineIndex=lineIndex)
            case TrimesterHeader():
                return StudentSnippet(
                    StudentId=None, StudentName=None,
                    FirstTrimester=rec.Date, LastTrimester=rec.Date,
                    LastMajor=None,
                    Credits=credits, WeightedGradeTotal=weightedGradeTotal,
                    FirstLineIndex=lineIndex, LastLineIndex=lineIndex)
            case ClassLine():
                credits[rec.Dept] += rec.Credits
                weightedGradeTotal[rec.Dept] += rec.Credits * rec.Grade
                return StudentSnippet(
                    StudentId=None, StudentName=None,
                    FirstTrimester=None, LastTrimester=None,
                    LastMajor=None,
                    Credits=credits, WeightedGradeTotal=weightedGradeTotal,
                    FirstLineIndex=lineIndex, LastLineIndex=lineIndex)
            case TrimesterFooter():
                return StudentSnippet(
                    StudentId=None, StudentName=None,
                    FirstTrimester=None, LastTrimester=None,
                    LastMajor=rec.Major,
                    Credits=credits, WeightedGradeTotal=weightedGradeTotal,
                    FirstLineIndex=lineIndex, LastLineIndex=lineIndex)
            case _:
                raise Exception("Unknown parsed row type")

    @staticmethod
    def completedFromSnippet(lhs):
        assert lhs.StudentId is not None and lhs.LastMajor is not None
        return CompletedStudent(
            StudentId=lhs.StudentId,
            StudentName=lhs.StudentName,
            SourceLines=lhs.LastLineIndex - lhs.FirstLineIndex + 1,
            LastMajor=lhs.LastMajor,
            Credits=lhs.Credits,
            WeightedGradeTotal=lhs.WeightedGradeTotal,
            FirstLineIndex=lhs.FirstLineIndex,
            LastLineIndex=lhs.LastLineIndex)

    @staticmethod
    def addSnippets(lhgroup, rhgroup):
        assert len(rhgroup) > 0
        if len(lhgroup) == 0:
            return rhgroup
        while len(rhgroup) > 0:
            lhs = lhgroup[-1]
            rhs = rhgroup[0]
            if rhs.StudentId is not None:
                if lhs.StudentId is not None:
                    lhgroup[-1] = StudentSnippetBuilder.completedFromSnippet(
                        lhs)
                lhgroup.append(rhs)
            else:
                assert lhs.LastLineIndex + 1 == rhs.FirstLineIndex
                credits = [0 for x in range(0, NumDepts)]
                weightedGradeTotal = [0 for x in range(0, NumDepts)]
                for dept in range(0, NumDepts):
                    credits[dept] = lhs.Credits[dept] + rhs.Credits[dept]
                    weightedGradeTotal[dept] = lhs.WeightedGradeTotal[dept] + \
                        rhs.WeightedGradeTotal[dept]
                lhgroup[-1] = StudentSnippet(
                    StudentId=lhs.StudentId,
                    StudentName=lhs.StudentName,
                    FirstTrimester=lhs.FirstTrimester if lhs.FirstTrimester is not None else rhs.FirstTrimester,
                    LastTrimester=rhs.LastTrimester,
                    LastMajor=rhs.LastMajor,
                    Credits=credits,
                    WeightedGradeTotal=weightedGradeTotal,
                    FirstLineIndex=lhs.FirstLineIndex,
                    LastLineIndex=rhs.LastLineIndex)
            rhgroup.pop(0)
        return lhgroup

    @staticmethod
    def addSnippetsWOCompleting(lhgroup, rhgroup):
        assert len(rhgroup) > 0
        if len(lhgroup) == 0:
            return rhgroup
        for rhs in rhgroup:
            lhs = lhgroup[-1]
            # print("Trying snip ending at %d against %d"%(lhs.LastLineIndex, rhs.FirstLineIndex))
            if rhs.StudentId is not None:
                lhgroup.append(rhs)
            else:
                if lhs.LastLineIndex + 1 != rhs.FirstLineIndex:
                    print('about to assert ',
                          lhs.LastLineIndex, rhs.FirstLineIndex)
                assert lhs.LastLineIndex + 1 == rhs.FirstLineIndex
                credits = [0 for x in range(0, NumDepts)]
                weightedGradeTotal = [0 for x in range(0, NumDepts)]
                for dept in range(0, NumDepts):
                    credits[dept] = lhs.Credits[dept] + rhs.Credits[dept]
                    weightedGradeTotal[dept] = lhs.WeightedGradeTotal[dept] + \
                        rhs.WeightedGradeTotal[dept]
                lhgroup[-1] = StudentSnippet(
                    StudentId=lhs.StudentId,
                    StudentName=lhs.StudentName,
                    FirstTrimester=lhs.FirstTrimester if lhs.FirstTrimester is not None else rhs.FirstTrimester,
                    LastTrimester=rhs.LastTrimester,
                    LastMajor=rhs.LastMajor,
                    Credits=credits,
                    WeightedGradeTotal=weightedGradeTotal,
                    FirstLineIndex=lhs.FirstLineIndex,
                    LastLineIndex=rhs.LastLineIndex)
        return lhgroup

    @staticmethod
    def gradeSummary(x):
        assert x.LastMajor is not None
        return StudentSummary(
            StudentId=x.StudentId,
            StudentName=x.StudentName,
            SourceLines=x.SourceLines,
            Major=x.LastMajor,
            GPA=sum(x.WeightedGradeTotal) / max(1, sum(x.Credits)),
            MajorGPA=x.WeightedGradeTotal[x.LastMajor] /
            max(1, x.Credits[x.LastMajor])
        )

# endregion
# region Preprocessor


def identifySectionUsingIntermediateFile(srcFilename):
    destFilename = f"{TEST_DATA_FILE_LOCATION}/temp.csv"
    if os.path.exists(destFilename):
        os.unlink(destFilename)
    reExtraType = re.compile("^S,")
    sectionId = -1
    with open(destFilename, "w") as outf:
        with open(srcFilename, "r") as inf:
            for line in inf:
                if reExtraType.match(line):
                    sectionId += 1
                assert sectionId >= 0
                outf.write(f"{sectionId},{line}")
    return destFilename
# endregion
