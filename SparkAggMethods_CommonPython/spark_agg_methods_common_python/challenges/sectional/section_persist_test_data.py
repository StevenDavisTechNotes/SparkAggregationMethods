import gc
import os
from abc import ABC
from pathlib import Path
from typing import Iterable

import pandas as pd
from pydantic import BaseModel, RootModel, TypeAdapter

from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    SectionDataSetDescription, StudentSummary, derive_expected_answer_data_file_path,
)


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
    ) -> pd.DataFrame:
        expected_answer_data_file_path = derive_expected_answer_data_file_path(data_description)
        if not os.path.exists(expected_answer_data_file_path):
            raise FileNotFoundError(f"Expected answer data file not found: {expected_answer_data_file_path}")
        return pd.read_parquet(expected_answer_data_file_path)

    @staticmethod
    def write_answer_file_sectional(
            data_description: SectionDataSetDescription,
            answer: pd.DataFrame,
    ) -> None:
        final_file_name = derive_expected_answer_data_file_path(
            data_description, temp_file=False)
        temp_file_name = derive_expected_answer_data_file_path(
            data_description, temp_file=True)
        if True:
            answer.to_parquet(temp_file_name, engine='pyarrow', index=False)
            os.rename(temp_file_name, final_file_name)
