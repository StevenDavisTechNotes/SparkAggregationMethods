import logging
import os
from dataclasses import dataclass
from typing import Any, NamedTuple

import pandas as pd
import pyarrow

from spark_agg_methods_common_python.perf_test_common import (
    LOCAL_TEST_DATA_FILE_LOCATION, Challenge, ChallengeMethodRegistrationBase,
    DataSetDescriptionBase, ExecutionParametersBase,
    NumericalToleranceExpectations, TChallengeMethodDelegate,
    TSolutionInterface,
)

logger = logging.getLogger(__name__)

# cSpell: ignore arange


SIX_TEST_CHALLENGES = [Challenge.BI_LEVEL, Challenge.CONDITIONAL, Challenge.VANILLA]
TARGET_PARQUET_BATCH_SIZE = 65536


class DataPointNT(NamedTuple):
    id: int
    grp: int
    subgrp: int
    A: float
    B: float
    C: float
    D: float
    E: float
    F: float


SIX_TEST_SOURCE_DATA_PYARROW_SCHEMA = pyarrow.schema([
    pyarrow.field("id", pyarrow.int32()),
    pyarrow.field("grp", pyarrow.int32()),
    pyarrow.field("subgrp", pyarrow.int32()),
    pyarrow.field("A", pyarrow.int32()),
    pyarrow.field("B", pyarrow.int32()),
    pyarrow.field("C", pyarrow.float64()),
    pyarrow.field("D", pyarrow.float64()),
    pyarrow.field("E", pyarrow.float64()),
    pyarrow.field("F", pyarrow.float64()),
])


@dataclass(frozen=True)
class SubTotalDC():
    running_count: int
    running_max_of_D: float
    running_sum_of_C: float
    running_sum_of_E_squared: float
    running_sum_of_E: float


class SubTotalNT(NamedTuple):
    running_sum_of_C: float
    running_count: int
    running_max_of_D: float | None
    running_sum_of_E_squared: float
    running_sum_of_E: float


@dataclass(frozen=True)
class TotalDC():
    mean_of_C: float
    max_of_D: float
    var_of_E: float
    var_of_E2: float


class SixTestDataSetDescription(DataSetDescriptionBase):
    num_grp_1: int
    num_grp_2: int
    points_per_index: int
    relative_cardinality_between_groupings: int

    def __init__(
            self,
            *,
            debugging_only: bool,
            num_grp_1: int,
            num_grp_2: int,
            points_per_index: int,
            size_code: str,
    ):
        num_source_rows = num_grp_1 * num_grp_2 * points_per_index
        relative_cardinality_between_groupings = num_grp_2 // num_grp_1
        super().__init__(
            debugging_only=debugging_only,
            num_source_rows=num_source_rows,
            size_code=size_code,
        )
        self.num_grp_1 = num_grp_1
        self.num_grp_2 = num_grp_2
        self.points_per_index = points_per_index
        self.relative_cardinality_between_groupings = relative_cardinality_between_groupings


@dataclass(frozen=True)
class SixTestDataSetAnswers():
    bi_level_answer: pd.DataFrame
    conditional_answer: pd.DataFrame
    vanilla_answer: pd.DataFrame

    def answer_for_challenge(
            self,
            challenge: Challenge,
    ) -> pd.DataFrame:
        match challenge:
            case Challenge.VANILLA:
                return self.vanilla_answer
            case Challenge.BI_LEVEL:
                return self.bi_level_answer
            case Challenge.CONDITIONAL:
                return self.conditional_answer
            case _:
                raise KeyError(f"Unknown challenge {challenge}")


@dataclass(frozen=True)
class SixTestExecutionParameters(ExecutionParametersBase):
    pass


@dataclass(frozen=True)
class SixTestDataSetWAnswers():
    num_source_rows: int
    src_num_partitions: int
    tgt_num_partitions_1_level: int
    tgt_num_partitions_2_level: int
    df_src: pd.DataFrame
    vanilla_answer: pd.DataFrame
    bilevel_answer: pd.DataFrame
    conditional_answer: pd.DataFrame


@dataclass(frozen=True)
class SixTestDataChallengeMethodRegistrationBase(
    ChallengeMethodRegistrationBase[TSolutionInterface, TChallengeMethodDelegate]
):
    numerical_tolerance: NumericalToleranceExpectations


@dataclass(frozen=True)
class SixTestSourceDataFilePaths():
    source_directory_path: str
    source_file_path_parquet_small_v1_files: str
    source_file_path_parquet_single_file: str
    source_file_path_csv: str

    @property
    def file_paths(self) -> list[str]:
        return [
            self.source_file_path_parquet_small_v1_files,
            self.source_file_path_parquet_single_file,
            self.source_file_path_csv,
        ]


def six_derive_expected_answer_data_file_paths_csv(
        data_description: SixTestDataSetDescription,
        *,
        temp_file: bool = False,
) -> dict[Challenge, str]:
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    repetition = data_description.points_per_index
    temp_postfix = "_temp" if temp_file else ""
    answer_file_names = {
        challenge: os.path.join(
            LOCAL_TEST_DATA_FILE_LOCATION,
            "SixField_Test_Data",
            f"{challenge}_answer_data_{num_grp_1}_{num_grp_2}_{repetition}{temp_postfix}.csv")
        for challenge in SIX_TEST_CHALLENGES
    }
    return answer_file_names


def six_derive_source_test_data_file_path(
        data_description: SixTestDataSetDescription,
        *,
        temp_file: bool = False,
) -> SixTestSourceDataFilePaths:
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    repetition = data_description.points_per_index
    temp_postfix = "_temp" if temp_file else ""
    source_directory_path = os.path.join(
        LOCAL_TEST_DATA_FILE_LOCATION,
        "SixField_Test_Data",
    )
    stem = os.path.join(
        source_directory_path,
        f"six_field_source_data_{num_grp_1}_{num_grp_2}_{repetition}{temp_postfix}",
    )
    return SixTestSourceDataFilePaths(
        source_directory_path=source_directory_path,
        source_file_path_parquet_small_v1_files=f"{stem}_spark",
        source_file_path_parquet_single_file=f"{stem}_modern.parquet",
        source_file_path_csv=f"{stem}.csv",
    )


def fetch_six_data_set_answer(
        challenge: Challenge,
        data_size: SixTestDataSetDescription,
        *,
        spark_logger: Any | None = None,
) -> pd.DataFrame:
    answer_file_path_csv = six_derive_expected_answer_data_file_paths_csv(data_size)[challenge]
    (spark_logger or logger).info(f"Loading answer from {answer_file_path_csv}")
    return pd.read_csv(answer_file_path_csv)
