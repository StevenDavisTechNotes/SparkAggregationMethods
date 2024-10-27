import datetime as dt
import os
import random
from abc import ABC
from pathlib import Path
from typing import Iterable

from pydantic import BaseModel, RootModel, TypeAdapter

from spark_agg_methods_common_python.challenges.sectional.section_nospark_logic import \
    section_nospark_logic
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    DATA_SIZE_LIST_SECTIONAL, LARGEST_EXPONENT_SECTIONAL,
    NUM_CLASSES_PER_TRIMESTER, NUM_DEPARTMENTS, NUM_TRIMESTERS,
    SectionDataSetDescription, StudentSummary, add_months_to_date_retracting,
    derive_expected_answer_data_file_path, derive_source_test_data_file_path)


class StudentSummaryPersisted(BaseModel):
    StudentId: int
    StudentName: str
    SourceLines: int
    GPA: float
    Major: int
    MajorGPA: float


AnswerFileFormatSectional = RootModel[list[StudentSummaryPersisted]]
ANSWER_FILE_FORMAT_SECTIONAL_TYPE_ADAPTER = TypeAdapter(AnswerFileFormatSectional)


def populate_data_files_sectional(
        *,
        data_description: SectionDataSetDescription,
) -> None:
    final_file_name = derive_source_test_data_file_path(
        data_description, temp_file=False)
    temp_file_name = derive_source_test_data_file_path(
        data_description, temp_file=True)
    if os.path.exists(final_file_name):
        os.unlink(final_file_name)
    Path(temp_file_name).parent.mkdir(parents=True, exist_ok=True)
    num_students = data_description.num_students
    num_trimesters = NUM_TRIMESTERS
    num_departments = NUM_DEPARTMENTS
    num_classes_per_trimester = NUM_CLASSES_PER_TRIMESTER
    with open(temp_file_name, "w") as f:
        print(f"Creating {final_file_name}")
        for student_id in range(1, num_students + 1):
            f.write(f"S,{student_id},John{student_id}\n")
            for trimester in range(1, num_trimesters + 1):
                dated = add_months_to_date_retracting(dt.datetime(2017, 1, 1), trimester)
                was_abroad = random.randint(0, 10) == 0
                major = (student_id %
                         num_departments) if trimester > 1 else num_departments - 1
                f.write(f"TH,{dated:%Y-%m-%d},{was_abroad}\n")
                trimester_credits = 0
                trimester_weighted_grades = 0
                for _i_class in range(1, num_classes_per_trimester + 1):
                    dept = random.randrange(0, num_departments)
                    grade = random.randint(1, 4)
                    credits = random.randint(1, 5)
                    f.write(f"C,{dept},{grade},{credits}\n")
                    trimester_credits += credits
                    trimester_weighted_grades += grade * credits
                gpa = trimester_weighted_grades / trimester_credits
                f.write(f"TF,{major},{gpa},{trimester_credits}\n")
    Path(final_file_name).parent.mkdir(parents=True, exist_ok=True)
    os.rename(temp_file_name, final_file_name)


class AnswerFileSectional(ABC):

    @staticmethod
    def read_answer_file_sectional(
            data_description: SectionDataSetDescription,
    ) -> list[StudentSummaryPersisted]:
        expected_answer_data_file_path = derive_expected_answer_data_file_path(data_description)
        if not os.path.exists(expected_answer_data_file_path):
            raise FileNotFoundError(f"Expected answer data file not found: {expected_answer_data_file_path}")
        with open(expected_answer_data_file_path, 'rb') as f:
            return AnswerFileFormatSectional.model_validate_json(f.read()).root

    @staticmethod
    def write_answer_file_sectional(
            data_description: SectionDataSetDescription,
            answer: Iterable[StudentSummary],
    ) -> None:
        final_file_name = derive_expected_answer_data_file_path(
            data_description, temp_file=False)
        temp_file_name = derive_expected_answer_data_file_path(
            data_description, temp_file=True)
        if os.path.exists(final_file_name):
            os.unlink(final_file_name)
        Path(final_file_name).parent.mkdir(parents=True, exist_ok=True)
        Path(temp_file_name).parent.mkdir(parents=True, exist_ok=True)
        answer_list = [StudentSummaryPersisted(**x._asdict()) for x in answer]
        with open(temp_file_name, 'wb') as f:
            f.write(ANSWER_FILE_FORMAT_SECTIONAL_TYPE_ADAPTER.dump_json(
                AnswerFileFormatSectional(answer_list)))
        os.rename(temp_file_name, final_file_name)


def populate_data_sets_base(
        make_new_files: bool,
):
    for i_scale in range(0, LARGEST_EXPONENT_SECTIONAL + 1):
        data_description = DATA_SIZE_LIST_SECTIONAL[i_scale]
        source_data_file_path = derive_source_test_data_file_path(
            data_description=data_description,
        )
        if make_new_files is True or os.path.exists(source_data_file_path) is False:
            populate_data_files_sectional(
                data_description=data_description,
            )
        answer_file_path = derive_expected_answer_data_file_path(
            data_description=data_description,
        )
        if make_new_files is True or os.path.exists(answer_file_path) is False:
            AnswerFileSectional.write_answer_file_sectional(
                data_description,
                section_nospark_logic(
                    data_description=data_description,
                )
            )
