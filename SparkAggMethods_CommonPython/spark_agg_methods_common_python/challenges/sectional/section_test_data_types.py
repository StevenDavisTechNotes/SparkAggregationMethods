import calendar
import datetime as dt
import inspect
import os
from dataclasses import dataclass
from typing import NamedTuple

from spark_agg_methods_common_python.perf_test_common import (
    TEST_DATA_FILE_LOCATION, DataSetDescriptionBase)

NUM_TRIMESTERS = 8
NUM_CLASSES_PER_TRIMESTER = 4
NUM_DEPARTMENTS = 4
SECTION_SIZE_MAXIMUM = (1 + NUM_TRIMESTERS * (1 + NUM_CLASSES_PER_TRIMESTER + 1))
LARGEST_EXPONENT_SECTIONAL = 7  # some can operate at 8 or above


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
    # for DataSetDescriptionBase
    debugging_only: bool
    num_source_rows: int
    size_code: str
    # for SectionDataSetDescription
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
        assert regressor_field_name in inspect.get_annotations(cls)
        return regressor_field_name


@dataclass(frozen=True)
class SectionDataSetBase():
    data_description: SectionDataSetDescription


DATA_SIZE_LIST_SECTIONAL = [
    SectionDataSetDescription(
        i_scale=i_scale,
        num_students=10**i_scale,
        section_size_max=SECTION_SIZE_MAXIMUM,
    )
    for i_scale in range(0, LARGEST_EXPONENT_SECTIONAL + 1)
]


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
        TEST_DATA_FILE_LOCATION,
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
        TEST_DATA_FILE_LOCATION,
        "Section_Test_Data",
        f"section_answer_data_{num_students}{temp_postfix}.json"
    )
