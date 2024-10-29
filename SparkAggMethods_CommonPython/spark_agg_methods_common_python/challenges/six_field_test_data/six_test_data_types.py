import math
import os
import pickle
import random
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, Challenge, ChallengeMethodRegistrationBase,
    DataSetDescriptionBase, ExecutionParametersBase,
    NumericalToleranceExpectations, TChallengeMethodDelegate,
    TSolutionInterface)
from spark_agg_methods_common_python.utils.utils import (always_true,
                                                         int_divide_round_up)

# cSpell: ignore arange

MAX_DATA_POINTS_PER_SPARK_PARTITION = 5 * 10**3
MAX_DATA_POINTS_PER_DASK_PARTITION = 1 * 10**5


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
    vanilla_answer: pd.DataFrame
    bilevel_answer: pd.DataFrame
    conditional_answer: pd.DataFrame

    def answer_for_challenge(
            self,
            challenge: Challenge,
    ) -> pd.DataFrame:
        match challenge:
            case Challenge.VANILLA:
                return self.vanilla_answer
            case Challenge.BI_LEVEL:
                return self.bilevel_answer
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


def populate_six_data_set_generic(
        engine: CalcEngine,
        exec_params: SixTestExecutionParameters,
        data_size: SixTestDataSetDescription,
) -> SixTestDataSetWAnswers:
    num_grp_1 = data_size.num_grp_1
    num_grp_2 = data_size.num_grp_2
    repetition = data_size.points_per_index
    num_data_points = num_grp_1 * num_grp_2 * repetition
    # Need to split this up, upfront, into many partitions
    # to avoid memory issues and
    # avoid preferential treatment of methods that don't repartition
    match engine:
        case CalcEngine.DASK:
            max_data_points_per_partition = MAX_DATA_POINTS_PER_DASK_PARTITION
        case CalcEngine.PYTHON_ONLY:
            max_data_points_per_partition = -1
        case CalcEngine.PYSPARK:
            max_data_points_per_partition = MAX_DATA_POINTS_PER_SPARK_PARTITION
        case _:
            raise ValueError(f"Unknown engine {engine}")
    src_num_partitions = (
        1 if max_data_points_per_partition < 0 else
        max(
            exec_params.default_parallelism,
            int_divide_round_up(
                num_data_points,
                max_data_points_per_partition,
            )
        )
    )
    staging_file_name_parquet = os.path.join(
        exec_params.test_data_folder_location,
        "SixField_Test_Data",
        f"SixFieldTestData_{num_grp_1}_{num_grp_2}_{repetition}.parquet")
    staging_file_name_csv = os.path.join(
        exec_params.test_data_folder_location,
        "SixField_Test_Data",
        f"SixFieldTestData_{num_grp_1}_{num_grp_2}_{repetition}.csv")
    if os.path.exists(staging_file_name_parquet) is False:
        generate_data_to_file(
            parquet_file_name=staging_file_name_parquet,
            csv_file_name=staging_file_name_csv,
            numGrp1=num_grp_1,
            numGrp2=num_grp_2,
            repetition=repetition,
        )
    df = pd.read_parquet(staging_file_name_parquet)
    assert len(df) == num_data_points
    vanilla_answer: pd.DataFrame = pd.DataFrame.from_records(
        [
            {
                "grp": grp,
                "subgrp": subgrp,
                "mean_of_C": df_cluster["C"].mean(),
                "max_of_D": df_cluster["D"].max(),
                "var_of_E": df_cluster["E"].var(ddof=0),
                "var_of_E2": df_cluster["E"].var(ddof=0),
            }
            for grp in range(num_grp_1)
            for subgrp in range(num_grp_2)
            if always_true(df_cluster := df[(df["grp"] == grp) & (df["subgrp"] == subgrp)])
        ])

    def hand_coded_variance(column: pd.Series) -> float:
        n = len(column)
        return (
            (
                (column * column).sum() / n -
                (column.sum() / n)**2
            )
            if n > 0 else
            math.nan
        )
    bilevel_answer = pd.DataFrame.from_records(
        [
            {
                "grp": grp,
                "mean_of_C": df_cluster.C.mean(),
                "max_of_D": df_cluster.D.max(),
                "avg_var_of_E": sub_group_e.var(ddof=0).mean(),
                "avg_var_of_E2": sub_group_e.agg(hand_coded_variance).mean(),
            }
            for grp in range(num_grp_1)
            if always_true(df_cluster := df[(df["grp"] == grp)])
            if always_true(sub_group_e := df_cluster.groupby(by=['subgrp'])['E'])
        ])
    conditional_answer = pd.DataFrame.from_records(
        [
            {
                "grp": grp,
                "subgrp": subgrp,
                "mean_of_C": df_cluster.C.mean(),
                "max_of_D": df_cluster.D.max(),
                "cond_var_of_E": negE.var(ddof=0),
                "cond_var_of_E2": hand_coded_variance(negE),
            }
            for grp in range(num_grp_1)
            for subgrp in range(num_grp_2)
            if always_true(df_cluster := df[(df["grp"] == grp) & (df["subgrp"] == subgrp)])
            if always_true(negE := df_cluster.loc[df_cluster["E"] < 0, 'E'])
        ])
    print(f"Using {num_grp_1}, {num_grp_2}, {repetition} "
          f"src_num_partitions={src_num_partitions} "
          f"each {num_grp_1 * num_grp_2 * repetition/src_num_partitions:.1f}")

    return SixTestDataSetWAnswers(
        num_source_rows=num_data_points,
        src_num_partitions=src_num_partitions,
        tgt_num_partitions_1_level=num_grp_1,
        tgt_num_partitions_2_level=num_grp_1 * num_grp_2,
        df_src=df,
        vanilla_answer=vanilla_answer,
        bilevel_answer=bilevel_answer,
        conditional_answer=conditional_answer,
    )


def generate_data_to_file(
        parquet_file_name: str,
        csv_file_name: str,
        numGrp1: int,
        numGrp2: int,
        repetition: int,
) -> None:
    num_data_points = numGrp1 * numGrp2 * repetition
    df = pd.DataFrame(
        data={
            "id": np.arange(num_data_points, dtype=np.int32),
            "grp": np.array([i // numGrp2 % numGrp1 for i in range(num_data_points)], dtype=np.int32),
            "subgrp": np.array([i % numGrp2 for i in range(num_data_points)], dtype=np.int32),
        },
        # np.array([[
        #     i,
        #     (i // numGrp2) % numGrp1,
        #     i % numGrp2,
        # ] for i in range(num_data_points)]),
        # columns=['id', 'grp', 'subgrp'],
        dtype=np.int32,
    )
    df['A'] = np.random.randint(1, repetition + 1, num_data_points, dtype=np.int32)
    df['B'] = np.random.randint(1, repetition + 1, num_data_points, dtype=np.int32)
    df['C'] = np.random.uniform(1, 10, num_data_points)
    df['D'] = np.random.uniform(1, 10, num_data_points)
    df['E'] = np.random.normal(0, 10, num_data_points)
    df['F'] = np.random.normal(1, 10, num_data_points)
    Path(parquet_file_name).parent.mkdir(parents=True, exist_ok=True)
    if True:
        tmp_file_name = f'{parquet_file_name}_t'
        with open(tmp_file_name, "wb") as fh:
            df.to_parquet(fh)
        os.rename(tmp_file_name, parquet_file_name)
    if num_data_points < 1000:
        tmp_file_name = f'{csv_file_name}_t'
        with open(tmp_file_name, "wb") as fh:
            df.to_csv(fh, index=False)
        os.rename(tmp_file_name, csv_file_name)
    elif os.path.exists(csv_file_name):
        os.remove(csv_file_name)


def generate_data_to_file_using_python_random(
        file_name: str,
        numGrp1: int,
        numGrp2: int,
        repetition: int,
) -> None:
    data_points = [
        DataPointNT(
            id=i,
            grp=(i // numGrp2) % numGrp1,
            subgrp=i % numGrp2,
            A=random.randint(1, repetition),
            B=random.randint(1, repetition),
            C=random.uniform(1, 10),
            D=random.uniform(1, 10),
            E=random.normalvariate(0, 10),
            F=random.normalvariate(1, 10))
        for i in range(0, numGrp1 * numGrp2 * repetition)]
    Path(file_name).parent.mkdir(parents=True, exist_ok=True)
    tmp_file_name = f'{file_name}_t'
    with open(tmp_file_name, "wb") as fh:
        pickle.dump(data_points, fh)
    os.rename(tmp_file_name, file_name)
