from abc import ABC
from typing import Any, Callable

import pandas as pd
from pydantic import BaseModel, RootModel, TypeAdapter
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    SectionDataSetDescription, section_derive_expected_answer_data_file_path,
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
ANSWER_FILE_VALIDATION_BATCH_SIZE: int = 100_000

_DTYPE_CHECKS: dict[Any, Callable[[Any], bool]] = {
    int: pd.api.types.is_integer_dtype,
    float: pd.api.types.is_float_dtype,
    str: pd.api.types.is_string_dtype,
}


def _validate_answer_schema(
        answer: pd.DataFrame,
        source: str,
) -> None:
    expected = {name: field.annotation for name, field in StudentSummaryPersisted.model_fields.items()}
    if list(answer.columns) != list(expected):
        raise ValueError(f"{source}: expected columns {list(expected)}, found {list(answer.columns)}")
    for name, annotation in expected.items():
        if not _DTYPE_CHECKS[annotation](answer[name].dtype):
            raise ValueError(f"{source}: column {name} has dtype {answer[name].dtype}, expected {annotation}")


def _validate_answer_rows(
        answer: pd.DataFrame,
) -> None:
    for start in range(0, len(answer), ANSWER_FILE_VALIDATION_BATCH_SIZE):
        ANSWER_FILE_FORMAT_SECTIONAL_TYPE_ADAPTER.validate_python(
            answer.iloc[start:start + ANSWER_FILE_VALIDATION_BATCH_SIZE].to_dict("records"))


class AnswerFileSectional(ABC):
    """Validation happens only at the file edges.

    Writing validates every row once, when the file is generated. Reading checks only the column names and dtypes
    (the parquet file is typed and is only written by this class), so loading a large answer stays cheap.
    """

    @staticmethod
    def read_answer_file_sectional(
            data_description: SectionDataSetDescription,
    ) -> pd.DataFrame:
        expected_answer_data_file_path = section_derive_expected_answer_data_file_path(data_description)
        if not expected_answer_data_file_path.exists():
            raise FileNotFoundError(f"Expected answer data file not found: {expected_answer_data_file_path}")
        answer = pd.read_parquet(
            path=expected_answer_data_file_path,
            engine="auto",
        )
        _validate_answer_schema(answer, str(expected_answer_data_file_path))
        return answer

    @staticmethod
    def write_answer_file_sectional(
            data_description: SectionDataSetDescription,
            answer: pd.DataFrame,
    ) -> None:
        final_file_name = section_derive_expected_answer_data_file_path(
            data_description, temp_file=False)
        temp_file_name = section_derive_expected_answer_data_file_path(
            data_description, temp_file=True)
        _validate_answer_schema(answer, str(final_file_name))
        _validate_answer_rows(answer)
        answer.to_parquet(
            temp_file_name,
            compression="zstd",
            index=False,
        )
        temp_file_name.replace(final_file_name)
