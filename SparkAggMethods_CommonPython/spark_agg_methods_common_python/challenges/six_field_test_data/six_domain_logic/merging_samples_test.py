
from typing import cast

import numpy as np
import pandas as pd
import pytest

from spark_agg_methods_common_python.challenges.six_field_test_data.six_domain_logic.merging_samples import (
    calculate_solution_progressively_from_iterable,
)

# cSpell: ignore arange, aggfunc
MAX_BATCH_SIZE: int = 3*3*1000


@pytest.fixture
def df_full():
    num_grp_1 = num_grp_2 = 3
    repetition = 1000
    full_range = range(0, MAX_BATCH_SIZE)
    df = pd.DataFrame(
        data={
            "id": np.arange(0, stop=MAX_BATCH_SIZE, dtype=np.int32),
            "grp": np.array([i // num_grp_2 % num_grp_1 for i in full_range], dtype=np.int32),
            "subgrp": np.array([i % num_grp_2 for i in full_range], dtype=np.int32),
        },
        dtype=np.int32,
    )
    df['A'] = np.random.randint(1, repetition + 1, MAX_BATCH_SIZE, dtype=np.int32)
    df['B'] = np.random.randint(1, repetition + 1, MAX_BATCH_SIZE, dtype=np.int32)
    df['C'] = np.random.uniform(1, 10, MAX_BATCH_SIZE)
    df['D'] = np.random.uniform(1, 10, MAX_BATCH_SIZE)
    df['E'] = np.random.normal(0, 10, MAX_BATCH_SIZE)
    df['F'] = np.random.normal(1, 10, MAX_BATCH_SIZE)
    return df


@pytest.fixture
def reference_answer(
    df_full: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, int | float]] = []
    for np_key, df_group in df_full.groupby(by=['grp', 'subgrp']):
        grp, subgrp = np_key  # type: ignore
        df_group_c = df_group.loc[:, 'C']
        df_group_d = df_group.loc[:, 'D']
        cond_df_group_e = df_group.loc[df_group["E"] < 0, 'E']
        uncond_df_group_e = df_group.loc[:, 'E']
        mean_c: float = df_group_c.mean()
        max_d: float = df_group_d.max()
        cond_var_e = cast(float, cond_df_group_e.var(ddof=0))
        uncond_var_e = cast(float, uncond_df_group_e.var(ddof=0))
        rows.append({
            "grp": int(grp),
            "subgrp": int(subgrp),
            "mean_of_C": mean_c,
            "max_of_D": max_d,
            "cond_var_of_E": cond_var_e,
            "uncond_var_of_E": uncond_var_e,
        })
    return pd.DataFrame.from_records(rows)


def test_different_batch_sizes_should_produce_similar_answers(
        df_full: pd.DataFrame,
        reference_answer: pd.DataFrame,
):
    for batch_size in [1, 2, 3, 1000, MAX_BATCH_SIZE-3, MAX_BATCH_SIZE-2, MAX_BATCH_SIZE-1, MAX_BATCH_SIZE]:
        # Arrange
        chunk_iterable = (df_full[i:i + batch_size] for i in range(0, MAX_BATCH_SIZE, batch_size))
        # Act
        num_data_points_visited, df_summary = calculate_solution_progressively_from_iterable(
            include_conditional=True,
            include_unconditional=True,
            chunk_iterable=chunk_iterable,
        )
        # Assert
        assert num_data_points_visited == MAX_BATCH_SIZE
        abs_diff = float(
            (reference_answer - df_summary)
            .abs().max().max())
        if abs_diff > 1e-12:
            print(f"batch_size: {batch_size}")
            print(df_summary)
            print(reference_answer)
            assert df_summary.equals(reference_answer)
