from collections import defaultdict
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet

from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    TARGET_PARQUET_BATCH_SIZE, SixTestDataSetDescription, six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import Challenge
from spark_agg_methods_common_python.utils.progressive_statistics.progressive_max import ProgressiveMax
from spark_agg_methods_common_python.utils.progressive_statistics.progressive_mean import ProgressiveMean
from spark_agg_methods_common_python.utils.progressive_statistics.progressive_variance import ProgressiveVariance

# cSpell: ignore aggfunc, ddof, np_key


def calculate_solution_intermediates_progressively(
        data_size: SixTestDataSetDescription,
        include_conditional: bool = False,
        include_unconditional: bool = False,
        batch_size: int = TARGET_PARQUET_BATCH_SIZE,
) -> tuple[int, pd.DataFrame]:
    source_file_names = six_derive_source_test_data_file_path(
        data_description=data_size,
    )
    parquet_file = pyarrow.parquet.ParquetFile(source_file_names.source_file_path_parquet_modern)
    chunk_iterable: Iterable[pd.DataFrame] = map(
        lambda x: x.to_pandas(),
        parquet_file.iter_batches(batch_size=batch_size)
    )
    return calculate_solution_progressively_from_iterable(
        include_conditional=include_conditional,
        include_unconditional=include_unconditional,
        chunk_iterable=chunk_iterable,
    )


class ProgressiveSixTestStage1BatchAccumulator:
    include_conditional: bool
    include_unconditional: bool
    uncond_count_subtotal: dict[tuple[int, int], int]
    mean_c_subtotal: dict[tuple[int, int], ProgressiveMean]
    max_d_subtotal: dict[tuple[int, int], ProgressiveMax]
    uncond_var_e_subtotal: dict[tuple[int, int], ProgressiveVariance]
    cond_var_e_subtotal: dict[tuple[int, int], ProgressiveVariance]

    def __init__(
            self,
            *,
            include_conditional: bool,
            include_unconditional: bool,
    ):
        self.include_conditional = include_conditional
        self.include_unconditional = include_unconditional
        self.uncond_count_subtotal = defaultdict(lambda: 0)
        self.mean_c_subtotal = defaultdict(lambda: ProgressiveMean())
        self.max_d_subtotal = defaultdict(lambda: ProgressiveMax())
        self.uncond_var_e_subtotal = defaultdict(lambda: ProgressiveVariance(ddof=0))
        self.cond_var_e_subtotal = defaultdict(lambda: ProgressiveVariance(ddof=0))

    def update(self, df_chunk: pd.DataFrame):
        for np_key, df_group in df_chunk.groupby(by=['grp', 'subgrp']):
            key = int(np_key[0]), int(np_key[1])
            df_group_c = df_group.loc[:, 'C']
            df_group_d = df_group.loc[:, 'D']
            self.uncond_count_subtotal[key] += len(df_group)
            self.mean_c_subtotal[key].update(df_group_c)
            self.max_d_subtotal[key].update(df_group_d)
            if self.include_conditional:
                cond_df_group_e = df_group.loc[df_group["E"] < 0, 'E']
                self.cond_var_e_subtotal[key].update(cond_df_group_e.to_numpy())
            if self.include_unconditional:
                uncond_df_group_e = df_group.loc[:, 'E']
                self.uncond_var_e_subtotal[key].update(uncond_df_group_e.to_numpy())

    def summary(self) -> tuple[int, pd.DataFrame]:
        num_data_points_visited = sum(self.uncond_count_subtotal.values())
        if num_data_points_visited > 0:
            df_summary = (
                pd.DataFrame.from_records(
                    data=[
                        {
                            "grp": key[0],
                            "subgrp": key[1],
                            "mean_of_C": self.mean_c_subtotal[key].mean,
                            "max_of_D": self.max_d_subtotal[key].max,
                        } | ({
                            "cond_var_of_E": self.cond_var_e_subtotal[key].variance,
                        } if self.include_conditional else {}
                        ) | ({
                            "uncond_var_of_E": self.uncond_var_e_subtotal[key].variance,
                        } if self.include_unconditional else {})
                        for key in self.uncond_count_subtotal
                    ],
                    columns=(
                        ["grp", "subgrp", "mean_of_C", "max_of_D"]
                        + (["cond_var_of_E"] if self.include_conditional else [])
                        + (["uncond_var_of_E"] if self.include_unconditional else [])
                    )
                )
                .sort_values(by=["grp", "subgrp"])
            )
        else:
            columns: list[pd.Series] = []
            columns += [
                pd.Series([], name="grp", dtype=int),
                pd.Series([], name="subgrp"),
                pd.Series([], name="mean_of_C"),
                pd.Series([], name="max_of_D"),
            ]
            if self.include_conditional:
                columns.append(pd.Series([], name="cond_var_of_E"))
            if self.include_unconditional:
                columns.append(pd.Series([], name="uncond_var_of_E"))
            df_summary = pd.concat(columns, axis=1)
        return num_data_points_visited, df_summary


def calculate_solution_progressively_from_iterable(
        *,
        include_conditional: bool,
        include_unconditional: bool,
        chunk_iterable,
) -> tuple[int, pd.DataFrame]:
    uncond_count_subtotal: dict[tuple[int, int], int] \
        = defaultdict(lambda: 0)
    mean_c_subtotal: dict[tuple[int, int], ProgressiveMean] \
        = defaultdict(lambda: ProgressiveMean())
    max_d_subtotal: dict[tuple[int, int], ProgressiveMax] \
        = defaultdict(lambda: ProgressiveMax())
    uncond_var_e_subtotal: dict[tuple[int, int], ProgressiveVariance] \
        = defaultdict(lambda: ProgressiveVariance(ddof=0))
    cond_var_e_subtotal: dict[tuple[int, int], ProgressiveVariance] \
        = defaultdict(lambda: ProgressiveVariance(ddof=0))
    for df_chunk in chunk_iterable:
        for np_key, df_group in df_chunk.groupby(by=['grp', 'subgrp']):
            key = int(np_key[0]), int(np_key[1])
            df_group_c = df_group.loc[:, 'C']
            df_group_d = df_group.loc[:, 'D']
            uncond_count_subtotal[key] += len(df_group)
            mean_c_subtotal[key].update(df_group_c)
            max_d_subtotal[key].update(df_group_d)
            if include_conditional:
                cond_df_group_e = df_group.loc[df_group["E"] < 0, 'E']
                cond_var_e_subtotal[key].update(cond_df_group_e.to_numpy())
            if include_unconditional:
                uncond_df_group_e = df_group.loc[:, 'E']
                uncond_var_e_subtotal[key].update(uncond_df_group_e.to_numpy())
    num_data_points_visited = sum(uncond_count_subtotal.values())
    df_summary = (
        pd.DataFrame.from_records([
            {
                "grp": key[0],
                "subgrp": key[1],
                "mean_of_C": mean_c_subtotal[key].mean,
                "max_of_D": max_d_subtotal[key].max,
            } | ({
                "cond_var_of_E": cond_var_e_subtotal[key].variance,
            } if include_conditional else {}
            ) | ({
                "uncond_var_of_E": uncond_var_e_subtotal[key].variance,
            } if include_unconditional else {})
            for key in uncond_count_subtotal
        ]
        )
        .sort_values(by=["grp", "subgrp"])
    )
    return num_data_points_visited, df_summary


def calculate_solutions_from_summary(
        data_size: SixTestDataSetDescription,
        challenges: list[Challenge],
        num_data_points_visited: int,
        df_summary: pd.DataFrame,
) -> dict[Challenge, pd.DataFrame]:
    include_bi_level = Challenge.BI_LEVEL in challenges
    include_conditional = Challenge.CONDITIONAL in challenges
    include_vanilla = Challenge.VANILLA in challenges
    num_grp_1 = data_size.num_grp_1
    num_grp_2 = data_size.num_grp_2
    repetition = data_size.points_per_index
    correct_num_data_points = num_grp_1 * num_grp_2 * repetition
    assert correct_num_data_points == num_data_points_visited
    result: dict[Challenge, pd.DataFrame] = dict()
    if include_bi_level:
        df_bi_level_answer: pd.DataFrame
        df_bi_level_answer = (
            df_summary
            .groupby("grp")  # pyright: ignore[reportOptionalMemberAccess]
            .agg(
                mean_of_C=pd.NamedAgg(column="mean_of_C", aggfunc=lambda x: np.mean(x)),
                max_of_D=pd.NamedAgg(column="max_of_D", aggfunc="max"),
                avg_var_of_E=pd.NamedAgg(column="uncond_var_of_E", aggfunc=lambda x: np.mean(x)),
            )
            .sort_index()
            .reset_index(drop=False)
        )
        df_bi_level_answer["avg_var_of_E2"] = df_bi_level_answer["avg_var_of_E"]
        result[Challenge.BI_LEVEL] = df_bi_level_answer
    if include_conditional:
        df_conditional_answer = df_summary.loc[:, ["grp", "subgrp", "mean_of_C", "max_of_D", "cond_var_of_E"]]
        df_conditional_answer["cond_var_of_E2"] = df_conditional_answer["cond_var_of_E"]
        result[Challenge.CONDITIONAL] = df_conditional_answer
    if include_vanilla:
        df_vanilla_answer = df_summary.loc[:, ["grp", "subgrp", "mean_of_C", "max_of_D", "uncond_var_of_E"]]
        df_vanilla_answer = df_vanilla_answer.rename(columns={"uncond_var_of_E": "var_of_E"})
        df_vanilla_answer["var_of_E2"] = df_vanilla_answer["var_of_E"]
        result[Challenge.VANILLA] = df_vanilla_answer
    return result


def calculate_solutions_progressively(
        data_size: SixTestDataSetDescription,
        challenges: list[Challenge],
) -> dict[Challenge, pd.DataFrame]:
    include_bi_level = Challenge.BI_LEVEL in challenges
    include_conditional = Challenge.CONDITIONAL in challenges
    include_vanilla = Challenge.VANILLA in challenges
    num_data_points_visited, df_summary = calculate_solution_intermediates_progressively(
        data_size=data_size,
        include_conditional=include_conditional,
        include_unconditional=include_bi_level or include_vanilla,
    )
    return calculate_solutions_from_summary(
        data_size=data_size,
        challenges=challenges,
        num_data_points_visited=num_data_points_visited,
        df_summary=df_summary,
    )
