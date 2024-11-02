import calendar
import datetime as dt
import os
import typing
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, NamedTuple

import pandas as pd

from spark_agg_methods_common_python.perf_test_common import (
    LOCAL_TEST_DATA_FILE_LOCATION, ChallengeMethodRegistrationBase, DataSetDescriptionBase, ExecutionParametersBase,
    TChallengeMethodDelegate, TSolutionInterface,
)

NUM_TRIMESTERS = 8
NUM_CLASSES_PER_TRIMESTER = 4
NUM_DEPARTMENTS = 4
SECTION_SIZE_MAXIMUM = (1 + NUM_TRIMESTERS * (1 + NUM_CLASSES_PER_TRIMESTER + 1))
LARGEST_EXPONENT_SECTIONAL = 7  # some can operate at 8 or above


class SolutionScale(StrEnum):
    WHOLE_FILE = 'whole_file'
    WHOLE_SECTION = 'whole_section'
    THREE_ROWS = 'three_rows'
    FINAL_SUMMARIES = 'final_summaries'
    SINGLE_LINE = 'singleline'


class StudentHeader(NamedTuple):
    StudentId: int
    StudentName: str


class TrimesterHeader(NamedTuple):
    Date: str
    WasAbroad: bool


class ClassLine(NamedTuple):
    Dept: int
    Credits: int
    Grade: int


class TrimesterFooter(NamedTuple):
    Major: int
    GPA: float
    Credits: int


TypedLine = StudentHeader | TrimesterHeader | ClassLine | TrimesterFooter


class StudentSummary(NamedTuple):
    StudentId: int
    StudentName: str
    SourceLines: int
    GPA: float
    Major: int
    MajorGPA: float


class LabeledTypedRow(NamedTuple):
    Index: int
    Value: TypedLine


class SectionDataSetDescription(DataSetDescriptionBase):
    i_scale: int
    num_students: int
    section_size_max: int

    def __init__(
            self,
            *,
            i_scale: int,
            num_students: int,
            section_size_max: int,
    ) -> None:
        debugging_only = False
        num_source_rows = num_students * section_size_max
        size_code = str(num_students)
        super().__init__(
            debugging_only=debugging_only,
            num_source_rows=num_source_rows,
            size_code=size_code,
        )
        self.i_scale = i_scale
        self.num_students = num_students
        self.section_size_max = section_size_max

    @classmethod
    def regressor_field_name(cls) -> str:
        regressor_field_name = "num_students"
        assert regressor_field_name in typing.get_type_hints(cls)
        return regressor_field_name


@dataclass(frozen=True)
class SectionDataSetBase():
    data_description: SectionDataSetDescription
    section_maximum: int
    correct_answer: pd.DataFrame


DATA_SIZE_LIST_SECTIONAL = [
    SectionDataSetDescription(
        i_scale=i_scale,
        num_students=10**i_scale,
        section_size_max=SECTION_SIZE_MAXIMUM,
    )
    for i_scale in range(0, LARGEST_EXPONENT_SECTIONAL + 1)
]


@dataclass(frozen=True)
class SectionExecutionParametersBase(ExecutionParametersBase):
    pass


@dataclass(frozen=True)
class SectionChallengeMethodRegistrationBase(
    ChallengeMethodRegistrationBase[
        TSolutionInterface, TChallengeMethodDelegate
    ]
):
    scale: SolutionScale


TChallengePythonAnswer = (
    Literal["infeasible"] | list[StudentSummary] | pd.DataFrame
)


def add_months_to_date_retracting(
        d: dt.date,
        add_months: int,
) -> dt.date:
    serial = d.year * 12 + (d.month - 1)
    serial += add_months
    year, month = serial // 12, serial % 12 + 1
    num_days = calendar.monthrange(year, month)[1]
    return dt.date(year, month, min(d.day, num_days))


def derive_source_test_data_file_path(
        data_description: SectionDataSetDescription,
        *,
        temp_file: bool = False,
) -> str:
    num_students = data_description.num_students
    temp_postfix = "_temp" if temp_file else ""
    return os.path.join(
        LOCAL_TEST_DATA_FILE_LOCATION,
        "Section_Test_Data",
        f"section_source_data_{num_students}{temp_postfix}.csv"
    )


def derive_expected_answer_data_file_path(
        data_description: SectionDataSetDescription,
        *,
        temp_file: bool = False,
) -> str:
    num_students = data_description.num_students
    temp_postfix = "_temp" if temp_file else ""
    return os.path.join(
        LOCAL_TEST_DATA_FILE_LOCATION,
        "Section_Test_Data",
        f"section_answer_data_{num_students}{temp_postfix}.parquet"
    )


def section_verify_correctness(
        data_set: SectionDataSetBase,
        df_found_students: pd.DataFrame,
) -> str | None:
    correct_answer = data_set.correct_answer
    if correct_answer.equals(df_found_students):
        return None
    diff = correct_answer.compare(df_found_students)
    return str(diff)
