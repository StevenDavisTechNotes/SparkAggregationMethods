import pytest

from src.challenges.sectional.domain_logic.section_snippet_subtotal_type import (
    FIRST_LAST_FIRST, FIRST_LAST_LAST, FIRST_LAST_NEITHER, CompletedStudent,
    StudentSnippet2,
)
from src.challenges.sectional.strategies.using_pyspark.section_pyspark_rdd_mappart_partials import (
    consolidate_snippets_in_partition,
)


@pytest.fixture
def student_history():
    return [
        (
            0,
            True,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=1,
                StudentName='John1',
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=0,
                LastLineIndex=0
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate='2017-02-01',
                LastTrimesterDate='2017-02-01',
                LastMajor=None,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=1,
                LastLineIndex=1
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 1],
                WeightedGradeTotal=[0, 0, 0, 1],
                FirstLineIndex=2,
                LastLineIndex=2
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[4, 0, 0, 0],
                WeightedGradeTotal=[4, 0, 0, 0],
                FirstLineIndex=3,
                LastLineIndex=3
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 3, 0],
                WeightedGradeTotal=[0, 0, 15, 0],
                FirstLineIndex=4,
                LastLineIndex=4
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 2],
                WeightedGradeTotal=[0, 0, 0, 8],
                FirstLineIndex=5,
                LastLineIndex=5
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=3,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=6,
                LastLineIndex=6
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate='2017-03-01',
                LastTrimesterDate='2017-03-01',
                LastMajor=None,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=7,
                LastLineIndex=7
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 1, 0, 0],
                WeightedGradeTotal=[0, 5, 0, 0],
                FirstLineIndex=8,
                LastLineIndex=8
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 1, 0],
                WeightedGradeTotal=[0, 0, 2, 0],
                FirstLineIndex=9,
                LastLineIndex=9
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 3, 0],
                WeightedGradeTotal=[0, 0, 6, 0],
                FirstLineIndex=10,
                LastLineIndex=10
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[4, 0, 0, 0],
                WeightedGradeTotal=[16, 0, 0, 0],
                FirstLineIndex=11,
                LastLineIndex=11
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=1,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=12,
                LastLineIndex=12
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate='2017-04-01',
                LastTrimesterDate='2017-04-01',
                LastMajor=None,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=13,
                LastLineIndex=13
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 3, 0],
                WeightedGradeTotal=[0, 0, 15, 0],
                FirstLineIndex=14,
                LastLineIndex=14
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[1, 0, 0, 0],
                WeightedGradeTotal=[2, 0, 0, 0],
                FirstLineIndex=15,
                LastLineIndex=15
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[2, 0, 0, 0],
                WeightedGradeTotal=[10, 0, 0, 0],
                FirstLineIndex=16,
                LastLineIndex=16
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[3, 0, 0, 0],
                WeightedGradeTotal=[15, 0, 0, 0],
                FirstLineIndex=17,
                LastLineIndex=17
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=1,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=18,
                LastLineIndex=18
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate='2017-05-01',
                LastTrimesterDate='2017-05-01',
                LastMajor=None,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=19,
                LastLineIndex=19
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 1],
                WeightedGradeTotal=[0, 0, 0, 4],
                FirstLineIndex=20,
                LastLineIndex=20
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 1, 0, 0],
                WeightedGradeTotal=[0, 5, 0, 0],
                FirstLineIndex=21,
                LastLineIndex=21
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 3],
                WeightedGradeTotal=[0, 0, 0, 15],
                FirstLineIndex=22,
                LastLineIndex=22
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 2, 0],
                WeightedGradeTotal=[0, 0, 4, 0],
                FirstLineIndex=23,
                LastLineIndex=23
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=1,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=24,
                LastLineIndex=24
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate='2017-06-01',
                LastTrimesterDate='2017-06-01',
                LastMajor=None,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=25,
                LastLineIndex=25
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 2, 0],
                WeightedGradeTotal=[0, 0, 4, 0],
                FirstLineIndex=26,
                LastLineIndex=26
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 2],
                WeightedGradeTotal=[0, 0, 0, 6],
                FirstLineIndex=27,
                LastLineIndex=27
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 4],
                WeightedGradeTotal=[0, 0, 0, 12],
                FirstLineIndex=28,
                LastLineIndex=28
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 1],
                WeightedGradeTotal=[0, 0, 0, 3],
                FirstLineIndex=29,
                LastLineIndex=29
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=1,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=30,
                LastLineIndex=30
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate='2017-07-01',
                LastTrimesterDate='2017-07-01',
                LastMajor=None,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=31,
                LastLineIndex=31
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 1],
                WeightedGradeTotal=[0, 0, 0, 3],
                FirstLineIndex=32,
                LastLineIndex=32
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 2],
                WeightedGradeTotal=[0, 0, 0, 10],
                FirstLineIndex=33,
                LastLineIndex=33
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[2, 0, 0, 0],
                WeightedGradeTotal=[2, 0, 0, 0],
                FirstLineIndex=34,
                LastLineIndex=34
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 2, 0, 0],
                WeightedGradeTotal=[0, 8, 0, 0],
                FirstLineIndex=35,
                LastLineIndex=35
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=1,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=36,
                LastLineIndex=36
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate='2017-08-01',
                LastTrimesterDate='2017-08-01',
                LastMajor=None,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=37,
                LastLineIndex=37
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 3],
                WeightedGradeTotal=[0, 0, 0, 6],
                FirstLineIndex=38,
                LastLineIndex=38
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[4, 0, 0, 0],
                WeightedGradeTotal=[16, 0, 0, 0],
                FirstLineIndex=39,
                LastLineIndex=39
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 3, 0, 0],
                WeightedGradeTotal=[0, 15, 0, 0],
                FirstLineIndex=40,
                LastLineIndex=40
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 3, 0],
                WeightedGradeTotal=[0, 0, 6, 0],
                FirstLineIndex=41,
                LastLineIndex=41
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=1,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=42,
                LastLineIndex=42
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate='2017-09-01',
                LastTrimesterDate='2017-09-01',
                LastMajor=None,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=43,
                LastLineIndex=43
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 3, 0],
                WeightedGradeTotal=[0, 0, 6, 0],
                FirstLineIndex=44,
                LastLineIndex=44
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 0, 4],
                WeightedGradeTotal=[0, 0, 0, 12],
                FirstLineIndex=45,
                LastLineIndex=45
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 0, 2, 0],
                WeightedGradeTotal=[0, 0, 2, 0],
                FirstLineIndex=46,
                LastLineIndex=46
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=None,
                Credits=[0, 4, 0, 0],
                WeightedGradeTotal=[0, 20, 0, 0],
                FirstLineIndex=47,
                LastLineIndex=47
            )
        ),
        (
            0,
            False,
            1,
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_NEITHER,
                StudentId=None,
                StudentName=None,
                FirstTrimesterDate=None,
                LastTrimesterDate=None,
                LastMajor=1,
                Credits=[0, 0, 0, 0],
                WeightedGradeTotal=[0, 0, 0, 0],
                FirstLineIndex=48,
                LastLineIndex=48
            )
        )
    ]


@pytest.fixture
def student_history_leader():
    return StudentSnippet2(
        FirstLastFlag=FIRST_LAST_FIRST,
        StudentId=None,
        StudentName=None,
        FirstTrimesterDate=None,
        LastTrimesterDate=None,
        LastMajor=None,
        Credits=None,
        WeightedGradeTotal=None,
        FirstLineIndex=-1,
        LastLineIndex=-1
    )


@pytest.fixture
def student_history_trailer():
    return StudentSnippet2(
        FirstLastFlag=FIRST_LAST_LAST,
        StudentId=None,
        StudentName=None,
        FirstTrimesterDate=None,
        LastTrimesterDate=None,
        LastMajor=None,
        Credits=None,
        WeightedGradeTotal=None,
        FirstLineIndex=49,
        LastLineIndex=49
    )


@pytest.fixture
def student_history_w_leader_trailer(
        student_history_leader: StudentSnippet2,
        student_history: list[tuple[int, bool, int, StudentSnippet2]],
        student_history_trailer: StudentSnippet2):
    return (
        [(
            0,
            True,
            1,
            student_history_leader
        )]
        + student_history
        + [(
            0,
            True,
            1,
            student_history_trailer

        )]
    )


@pytest.fixture
def baked_snippet():
    return StudentSnippet2(
        FirstLastFlag=FIRST_LAST_NEITHER,
        FirstLineIndex=0,
        LastLineIndex=48,
        StudentId=1,
        StudentName='John1',
        FirstTrimesterDate='2017-02-01',
        LastTrimesterDate='2017-09-01',
        LastMajor=1,
        Credits=[20, 11, 22, 24],
        WeightedGradeTotal=[65, 53, 60, 80],
    )


@pytest.fixture
def completed_student():
    return CompletedStudent(
        StudentId=1,
        StudentName='John1',
        LastMajor=1,
        Credits=[20, 11, 22, 24],
        WeightedGradeTotal=[65, 53, 60, 80],
        FirstLineIndex=0,
        LastLineIndex=48,
    )


def test_one_student_pass_1_not_leader_trailer(
        student_history: list[tuple[int, bool, int, StudentSnippet2]],
        baked_snippet: StudentSnippet2,
):
    result = list(consolidate_snippets_in_partition(student_history))
    assert len(result) == 1
    is_complete, snippet = result[0]
    assert is_complete is False
    assert snippet == baked_snippet


def test_one_student_pass_1_w_leader_trailer(
        student_history_w_leader_trailer: list[tuple[int, bool, int, StudentSnippet2]],
        student_history_leader: list[tuple[int, bool, int, StudentSnippet2]],
        student_history_trailer: list[tuple[int, bool, int, StudentSnippet2]],
        completed_student: StudentSnippet2,
):
    result = list(consolidate_snippets_in_partition(student_history_w_leader_trailer))
    assert len(result) == 3

    is_complete, snippet = result[0]
    assert is_complete is False
    assert snippet == student_history_leader

    is_complete, snippet = result[1]
    assert is_complete is True
    assert snippet == completed_student

    is_complete, snippet = result[2]
    assert is_complete is False
    assert snippet == student_history_trailer
