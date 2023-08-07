import collections
from typing import List

from SectionPerfTest.SectionTypeDefs import (
    ClassLine, NumDepts, StudentHeader, StudentSummary, TrimesterFooter, TrimesterHeader, TypedLine)

StudentSnippet = collections.namedtuple(
    "StudentSnippet",
    ["StudentId", "StudentName",
     "FirstTrimester", "LastTrimester", "LastMajor", "Credits", "WeightedGradeTotal",
     "FirstLineIndex", "LastLineIndex"])
CompletedStudent = collections.namedtuple(
    "CompletedStudent",
    ["StudentId", "StudentName", "SourceLines", "LastMajor", "Credits", "WeightedGradeTotal",
     "FirstLineIndex", "LastLineIndex"])


class StudentSnippetBuilder:
    @staticmethod
    def studentSnippetFromTypedRow(
        lineIndex: int,
        rec: TypedLine,
    ) -> StudentSnippet:
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
    def addSnippetsWOCompleting(
            lhgroup: List[StudentSnippet],
            rhgroup: List[StudentSnippet]
    ) -> List[StudentSnippet]:
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
    def gradeSummary(
            x: CompletedStudent
    ) -> StudentSummary:
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
