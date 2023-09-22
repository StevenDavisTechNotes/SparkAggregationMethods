from SectionPerfTest.SectionSnippetSubtotal import (
    FIRST_LAST_NEITHER, CompletedStudent, StudentSnippet2, completeSnippets2)

import pytest


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


def test_full(baked_snippet, completed_student):
    completedItems, remaining_snippets = completeSnippets2(
        baked_snippet,
        front_is_clean=True,
        back_is_clean=True)
    assert completedItems == [completed_student]
    assert remaining_snippets == []


def test_maybe_not_done_yet(baked_snippet):
    completedItems, remaining_snippets = completeSnippets2(
        baked_snippet,
        front_is_clean=True,
        back_is_clean=False)
    assert completedItems == []
    assert remaining_snippets == [baked_snippet]


def test_maybe_not_started_yet(baked_snippet):
    for back_is_clean in [True, False]:
        completedItems, remaining_snippets = completeSnippets2(
            baked_snippet,
            front_is_clean=False,
            back_is_clean=back_is_clean)
        assert completedItems == []
        assert remaining_snippets == [baked_snippet]


if __name__ == "__main__":
    print("here")
