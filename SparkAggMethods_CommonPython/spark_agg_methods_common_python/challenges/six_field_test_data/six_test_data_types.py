import os
from dataclasses import dataclass
from typing import NamedTuple

import pandas as pd

from spark_agg_methods_common_python.perf_test_common import (
    LOCAL_TEST_DATA_FILE_LOCATION, SIX_TEST_CHALLENGES, Challenge, ChallengeMethodRegistrationBase,
    DataSetDescriptionBase, ExecutionParametersBase, NumericalToleranceExpectations, TChallengeMethodDelegate,
    TSolutionInterface,
)

# cSpell: ignore arange


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


@dataclass(frozen=True)
class DataPointDC():
    id: int
    grp: int
    subgrp: int
    A: float
    B: float
    C: float
    D: float
    E: float
    F: float


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


def six_derive_source_test_data_file_path(
        data_description: SixTestDataSetDescription,
        *,
        temp_file: bool = False,
) -> tuple[str, str]:
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    repetition = data_description.points_per_index
    temp_postfix = "_temp" if temp_file else ""
    source_file_name_parquet = os.path.join(
        LOCAL_TEST_DATA_FILE_LOCATION,
        "SixField_Test_Data",
        f"six_field_source_data_{num_grp_1}_{num_grp_2}_{repetition}{temp_postfix}.parquet")
    source_file_name_csv = os.path.join(
        LOCAL_TEST_DATA_FILE_LOCATION,
        "SixField_Test_Data",
        f"six_field_source_data_{num_grp_1}_{num_grp_2}_{repetition}{temp_postfix}.csv")
    return source_file_name_parquet, source_file_name_csv


def six_derive_expected_answer_data_file_path(
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


def fetch_six_data_set_answer(
        challenge: Challenge,
        data_size: SixTestDataSetDescription,
) -> pd.DataFrame:
    answer_file_names = six_derive_expected_answer_data_file_path(data_size)[challenge]
    return pd.read_csv(answer_file_names)


# def populate_six_data_set_generic(
#         # engine: CalcEngine,
#         # exec_params: SixTestExecutionParameters,
#         data_size: SixTestDataSetDescription,
#         make_new_files: bool,
# ) -> SixTestDataSetWAnswers:
#     num_grp_1 = data_size.num_grp_1
#     num_grp_2 = data_size.num_grp_2
#     repetition = data_size.points_per_index
#     # num_data_points = num_grp_1 * num_grp_2 * repetition
#     # Need to split this up, upfront, into many partitions
#     # to avoid memory issues and
#     # avoid preferential treatment of methods that don't repartition
#     # match engine:
#     #     case CalcEngine.DASK:
#     #         max_data_points_per_partition = MAX_DATA_POINTS_PER_DASK_PARTITION
#     #     case CalcEngine.PYTHON_ONLY:
#     #         max_data_points_per_partition = -1
#     #     case CalcEngine.PYSPARK:
#     #         max_data_points_per_partition = MAX_DATA_POINTS_PER_SPARK_PARTITION
#     #     case _:
#     #         raise ValueError(f"Unknown engine {engine}")
#     # src_num_partitions = (
#     #     1 if max_data_points_per_partition < 0 else
#     #     max(
#     #         exec_params.default_parallelism,
#     #         int_divide_round_up(
#     #             num_data_points,
#     #             max_data_points_per_partition,
#     #         )
#     #     )
#     # )

#     # staging_file_name_parquet = os.path.join(
#     #     exec_params.test_data_folder_location,
#     #     "SixField_Test_Data",
#     #     f"SixFieldTestData_{num_grp_1}_{num_grp_2}_{repetition}.parquet")
#     # staging_file_name_csv = os.path.join(
#     #     exec_params.test_data_folder_location,
#     #     "SixField_Test_Data",
#     #     f"SixFieldTestData_{num_grp_1}_{num_grp_2}_{repetition}.csv")
#     source_file_name_parquet, source_file_name_csv = six_derive_source_test_data_file_path(
#         data_description=data_size,
#     )
#     answer_file_names = six_derive_expected_answer_data_file_path(data_size)
#     df = pd.read_parquet(source_file_name_parquet)
#     answer_file_names = six_derive_expected_answer_data_file_path(data_size)
#     if make_new_files or any(not os.path.exists(x) for x in answer_file_names):
#         generate_answer_file(data_size)
