import os
from abc import ABC
from pathlib import Path
from typing import Iterable

from pydantic import BaseModel, RootModel, TypeAdapter

from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    SectionDataSetDescription, StudentSummary,
    derive_expected_answer_data_file_path)


class StudentSummaryPersisted(BaseModel):
    StudentId: int
    StudentName: str
    SourceLines: int
    GPA: float
    Major: int
    MajorGPA: float


AnswerFileFormatSectional = RootModel[list[StudentSummaryPersisted]]
ANSWER_FILE_FORMAT_SECTIONAL_TYPE_ADAPTER = TypeAdapter(AnswerFileFormatSectional)


class AnswerFileSectional(ABC):

    @staticmethod
    def read_answer_file_sectional(
            data_description: SectionDataSetDescription,
    ) -> list[StudentSummary]:
        expected_answer_data_file_path = derive_expected_answer_data_file_path(data_description)
        if not os.path.exists(expected_answer_data_file_path):
            raise FileNotFoundError(f"Expected answer data file not found: {expected_answer_data_file_path}")
        with open(expected_answer_data_file_path, 'rb') as f:
            return [
                StudentSummary(**x.model_dump())
                for x in
                AnswerFileFormatSectional.model_validate_json(f.read())
                .root
            ]

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
