import logging
import math
import os
import pickle
import random
from pathlib import Path

import numpy as np
import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    DataPointNT, SixTestDataSetDescription, six_derive_expected_answer_data_file_path,
    six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import Challenge
from spark_agg_methods_common_python.utils.utils import always_true

# cSpell: ignore arange

logger = logging.getLogger(__name__)


def generate_answer_file(data_size: SixTestDataSetDescription) -> None:
    answer_file_names = six_derive_expected_answer_data_file_path(data_size)
    for file_name in answer_file_names.values():
        if os.path.exists(file_name):
            os.remove(file_name)
    source_file_name_parquet, source_file_name_csv = six_derive_source_test_data_file_path(
        data_description=data_size,
    )
    df = pd.read_parquet(source_file_name_parquet)
    num_grp_1 = data_size.num_grp_1
    num_grp_2 = data_size.num_grp_2
    repetition = data_size.points_per_index
    num_data_points = num_grp_1 * num_grp_2 * repetition
    assert len(df) == num_data_points

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
    bi_level_answer = pd.DataFrame.from_records(
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
    logger.info(f"Generating 6 data for {num_grp_1}, {num_grp_2}, {repetition} ")
    bi_level_answer.to_csv(answer_file_names[Challenge.BI_LEVEL], index=False)
    conditional_answer.to_csv(answer_file_names[Challenge.CONDITIONAL], index=False)
    vanilla_answer.to_csv(answer_file_names[Challenge.VANILLA], index=False)


def generate_source_data_file(
        data_description: SixTestDataSetDescription,
) -> None:
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    repetition = data_description.points_per_index
    source_file_name_parquet, source_file_name_csv = six_derive_source_test_data_file_path(
        data_description=data_description,
        temp_file=False,
    )
    temp_source_file_name_parquet, temp_source_file_name_csv = six_derive_source_test_data_file_path(
        data_description=data_description,
        temp_file=True,
    )
    if os.path.exists(source_file_name_parquet):
        os.remove(source_file_name_parquet)
    if os.path.exists(source_file_name_csv):
        os.remove(source_file_name_csv)
    if os.path.exists(temp_source_file_name_parquet):
        os.remove(temp_source_file_name_parquet)
    if os.path.exists(temp_source_file_name_csv):
        os.remove(temp_source_file_name_csv)
    num_data_points = num_grp_1 * num_grp_2 * repetition
    df = pd.DataFrame(
        data={
            "id": np.arange(num_data_points, dtype=np.int32),
            "grp": np.array([i // num_grp_2 % num_grp_1 for i in range(num_data_points)], dtype=np.int32),
            "subgrp": np.array([i % num_grp_2 for i in range(num_data_points)], dtype=np.int32),
        },
        dtype=np.int32,
    )
    df['A'] = np.random.randint(1, repetition + 1, num_data_points, dtype=np.int32)
    df['B'] = np.random.randint(1, repetition + 1, num_data_points, dtype=np.int32)
    df['C'] = np.random.uniform(1, 10, num_data_points)
    df['D'] = np.random.uniform(1, 10, num_data_points)
    df['E'] = np.random.normal(0, 10, num_data_points)
    df['F'] = np.random.normal(1, 10, num_data_points)
    Path(source_file_name_parquet).parent.mkdir(parents=True, exist_ok=True)
    if True:
        df.to_parquet(temp_source_file_name_parquet, engine='pyarrow', index=False)
        os.rename(temp_source_file_name_parquet, source_file_name_parquet)
    if True:  # num_data_points < 1000:
        with open(temp_source_file_name_csv, "wb") as fh:
            df.to_csv(fh, index=False)
        os.rename(temp_source_file_name_csv, source_file_name_csv)


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


def six_generate_data_file(
        *,
        data_description: SixTestDataSetDescription,
        make_new_files: bool,
) -> None:
    source_file_name_parquet, source_file_name_csv = six_derive_source_test_data_file_path(
        data_description=data_description,
    )
    if (
        make_new_files
        or not os.path.exists(source_file_name_parquet)
        or not os.path.exists(source_file_name_csv)
    ):
        generate_source_data_file(data_description)
    answer_file_names = six_derive_expected_answer_data_file_path(data_description)
    if make_new_files or any(not os.path.exists(x) for x in answer_file_names.values()):
        generate_answer_file(data_description)
