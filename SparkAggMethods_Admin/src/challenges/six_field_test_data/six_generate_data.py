import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestDataSetDescription, six_derive_expected_answer_data_file_path, six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import Challenge
from spark_agg_methods_common_python.utils.progressive_statistics.progressive_max import ProgressiveMax
from spark_agg_methods_common_python.utils.progressive_statistics.progressive_mean import ProgressiveMean
from spark_agg_methods_common_python.utils.progressive_statistics.progressive_variance import ProgressiveVariance

# cSpell: ignore arange, aggfunc

logger = logging.getLogger(__name__)


def generate_answer_file(data_size: SixTestDataSetDescription) -> None:
    answer_file_names = six_derive_expected_answer_data_file_path(data_size)
    for file_name in answer_file_names.values():
        if os.path.exists(file_name):
            os.remove(file_name)
    source_file_name_parquet, source_file_name_csv = six_derive_source_test_data_file_path(
        data_description=data_size,
    )
    num_grp_1 = data_size.num_grp_1
    num_grp_2 = data_size.num_grp_2
    repetition = data_size.points_per_index
    correct_num_data_points = num_grp_1 * num_grp_2 * repetition
    parquet_file = pyarrow.parquet.ParquetFile(source_file_name_parquet)
    logger.info(f"Generating 6 data for {num_grp_1}, {num_grp_2}, {repetition} ")

    def calculate_by_batch() -> pd.DataFrame:
        uncond_count_subtotal: dict[tuple[int, int], int] = dict()
        mean_c_subtotal: dict[tuple[int, int], ProgressiveMean] = dict()
        max_d_subtotal: dict[tuple[int, int], ProgressiveMax] = dict()
        uncond_var_e_subtotal: dict[tuple[int, int], ProgressiveVariance] = dict()
        cond_var_e_subtotal: dict[tuple[int, int], ProgressiveVariance] = dict()
        for chunk in parquet_file.iter_batches(batch_size=10000):
            df = chunk.to_pandas()
            for np_key, df_group in df.groupby(by=['grp', 'subgrp']):
                df_group_c = df_group.loc[:, 'C']
                df_group_d = df_group.loc[:, 'D']
                uncond_df_group_e = df_group.loc[:, 'E']
                cond_df_group = df_group.loc[df_group["E"] < 0, 'E']
                key = int(np_key[0]), int(np_key[1])
                if key not in uncond_count_subtotal:
                    uncond_count_subtotal[key] = 0
                    mean_c_subtotal[key] = ProgressiveMean()
                    max_d_subtotal[key] = ProgressiveMax()
                    uncond_var_e_subtotal[key] = ProgressiveVariance(ddof=0)
                    cond_var_e_subtotal[key] = ProgressiveVariance(ddof=0)
                uncond_count_subtotal[key] += len(df_group)
                mean_c_subtotal[key].update(df_group_c)
                max_d_subtotal[key].update(df_group_d)
                uncond_var_e_subtotal[key].update(uncond_df_group_e)
                cond_var_e_subtotal[key].update(cond_df_group)
        assert correct_num_data_points == sum(uncond_count_subtotal.values())
        return pd.DataFrame.from_records([
            {
                "grp": grp,
                "subgrp": subgrp,
                "mean_of_C": mean_c_subtotal[key].mean,
                "max_of_D": max_d_subtotal[key].max,
                "uncond_var_of_E": uncond_var_e_subtotal[key].variance,
                "cond_var_of_E": cond_var_e_subtotal[key].variance,
            }
            for grp in range(num_grp_1)
            for subgrp in range(num_grp_2)
            if (key := (grp, subgrp)) is not None
        ])
    df_summary = calculate_by_batch()

    df_bi_level_answer = (
        df_summary.groupby("grp")
        .agg(
            mean_of_C=pd.NamedAgg(column="mean_of_C", aggfunc=lambda x: np.mean(x)),
            max_of_D=pd.NamedAgg(column="max_of_D", aggfunc="max"),
            avg_var_of_E=pd.NamedAgg(column="uncond_var_of_E", aggfunc=lambda x: np.mean(x)),
        )
        .sort_index()
        .reset_index(drop=False)
    )
    df_bi_level_answer["avg_var_of_E2"] = df_bi_level_answer["avg_var_of_E"]
    df_conditional_answer = df_summary.loc[:, ["grp", "subgrp", "mean_of_C", "max_of_D", "cond_var_of_E"]]
    df_conditional_answer["cond_var_of_E2"] = df_conditional_answer["cond_var_of_E"]
    df_vanilla_answer = df_summary.loc[:, ["grp", "subgrp", "mean_of_C", "max_of_D", "uncond_var_of_E"]]
    df_vanilla_answer = df_vanilla_answer.rename(columns={"uncond_var_of_E": "var_of_E"})
    df_vanilla_answer["var_of_E2"] = df_vanilla_answer["var_of_E"]

    df_bi_level_answer.to_csv(answer_file_names[Challenge.BI_LEVEL], index=False)
    df_conditional_answer.to_csv(answer_file_names[Challenge.CONDITIONAL], index=False)
    df_vanilla_answer.to_csv(answer_file_names[Challenge.VANILLA], index=False)


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
    Path(source_file_name_parquet).parent.mkdir(parents=True, exist_ok=True)
    parquet_file = pyarrow.parquet.ParquetWriter(temp_source_file_name_parquet, pyarrow.schema([
        pyarrow.field("id", pyarrow.int32()),
        pyarrow.field("grp", pyarrow.int32()),
        pyarrow.field("subgrp", pyarrow.int32()),
        pyarrow.field("A", pyarrow.int32()),
        pyarrow.field("B", pyarrow.int32()),
        pyarrow.field("C", pyarrow.float64()),
        pyarrow.field("D", pyarrow.float64()),
        pyarrow.field("E", pyarrow.float64()),
        pyarrow.field("F", pyarrow.float64()),
    ]))
    for i_batch_start in range(0, num_data_points, 10000):
        i_batch_end = min(i_batch_start + 10000, num_data_points)
        batch_size = i_batch_end - i_batch_start
        batch_range = range(i_batch_start, i_batch_end)
        df = pd.DataFrame(
            data={
                "id": np.arange(start=i_batch_start, stop=i_batch_end, dtype=np.int32),
                "grp": np.array([i // num_grp_2 % num_grp_1 for i in batch_range], dtype=np.int32),
                "subgrp": np.array([i % num_grp_2 for i in batch_range], dtype=np.int32),
            },
            dtype=np.int32,
        )
        df['A'] = np.random.randint(1, repetition + 1, batch_size, dtype=np.int32)
        df['B'] = np.random.randint(1, repetition + 1, batch_size, dtype=np.int32)
        df['C'] = np.random.uniform(1, 10, batch_size)
        df['D'] = np.random.uniform(1, 10, batch_size)
        df['E'] = np.random.normal(0, 10, batch_size)
        df['F'] = np.random.normal(1, 10, batch_size)
        if num_data_points < 1000:
            with open(temp_source_file_name_csv, "wb") as fh:
                df.to_csv(fh, index=False)
            os.rename(temp_source_file_name_csv, source_file_name_csv)
        parquet_file.write_table(pyarrow.table(df))
    parquet_file.close()
    os.rename(temp_source_file_name_parquet, source_file_name_parquet)


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
