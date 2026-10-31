from typing import cast

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import TARGET_ROWS_PER_PARTITION
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer

KEYS = ["grp", "subgrp"]

# pyarrow.compute functions are generated at import time, so type checkers cannot see them
pc_multiply = pc.multiply  # ty: ignore[unresolved-attribute]  # pyright: ignore[reportAttributeAccessIssue]


def merge_partials(partials: list[pa.Table]) -> pd.DataFrame:
    """Combine per-batch partial aggregates into one row per (grp, subgrp), ordered by key."""
    df = pa.concat_tables(partials).to_pandas()
    df = df.rename(columns={
        "E_count": "n", "C_sum": "sum_C", "D_max": "max_D", "E_sum": "sum_E", "E2_sum": "sum_E2", "E_variance": "var_E",
    })
    by_key = df.groupby(by=KEYS)
    n_total = by_key["n"].transform("sum")
    mean_total = by_key["sum_E"].transform("sum") / n_total
    # Chan et al.: pooled M2 = sum of within-batch M2 plus between-batch spread around the pooled mean
    df["m2"] = df["n"] * df["var_E"] + df["n"] * (df["sum_E"] / df["n"] - mean_total) ** 2
    return df.groupby(by=KEYS).agg(
        n=("n", "sum"),
        sum_C=("sum_C", "sum"),
        max_D=("max_D", "max"),
        sum_E=("sum_E", "sum"),
        sum_E2=("sum_E2", "sum"),
        m2=("m2", "sum"),
    )


def bi_level_py_st_pa_grp_batches(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    partials: list[pa.Table] = []
    for batch in pq.ParquetFile(data_set.data.source_file_path_parquet).iter_batches(
            batch_size=TARGET_ROWS_PER_PARTITION,
            columns=["grp", "subgrp", "C", "D", "E"],
    ):
        table = pa.Table.from_batches([batch])
        e = table["E"]
        table = table.append_column("E2", pc_multiply(e, e))
        partials.append(table.group_by(KEYS).aggregate([
            ("E", "count"),
            ("C", "sum"),
            ("D", "max"),
            ("E", "sum"),
            ("E2", "sum"),
            ("E", "variance", pc.VarianceOptions(ddof=0)),
        ]))
    merged = merge_partials(partials)
    n = merged["n"]
    df_subgroup = pd.DataFrame({
        "n": n,
        "sum_C": merged["sum_C"],
        "max_D": merged["max_D"],
        "var_of_E": merged["m2"] / n,
        "var_of_E2": merged["sum_E2"] / n - (merged["sum_E"] / n) ** 2,
    })
    by_grp = df_subgroup.groupby(level="grp")
    result_df = (
        pd.DataFrame({
            "mean_of_C": (
                cast(pd.Series, by_grp["sum_C"].sum())
                / cast(pd.Series, by_grp["n"].sum())
            ),
            "max_of_D": by_grp["max_D"].max(),
            "avg_var_of_E": by_grp["var_of_E"].mean(),
            "avg_var_of_E2": by_grp["var_of_E2"].mean(),
        })
        .reset_index()
    )
    return result_df
