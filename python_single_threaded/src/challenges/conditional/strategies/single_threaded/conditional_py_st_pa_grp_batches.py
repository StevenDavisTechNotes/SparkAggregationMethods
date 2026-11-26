import numpy as np
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
pc_if_else = pc.if_else  # ty: ignore[unresolved-attribute]  # pyright: ignore[reportAttributeAccessIssue]
pc_less = pc.less  # ty: ignore[unresolved-attribute]  # pyright: ignore[reportAttributeAccessIssue]
pc_multiply = pc.multiply  # ty: ignore[unresolved-attribute]  # pyright: ignore[reportAttributeAccessIssue]


def merge_partials(partials: list[pa.Table]) -> pd.DataFrame:
    """Combine per-batch partial aggregates into one row per (grp, subgrp), ordered by key."""
    df = pa.concat_tables(partials).to_pandas()
    df = df.rename(columns={
        "C_sum": "sum_C", "D_max": "max_D", "E_count": "n",
        "negE_count": "n_neg", "negE_sum": "sum_neg_E", "negE2_sum": "sum_neg_E2", "negE_variance": "var_neg_E",
    })
    # a batch without negative E in some group leaves nulls: they contribute nothing
    df[["sum_neg_E", "sum_neg_E2", "var_neg_E"]] = df[["sum_neg_E", "sum_neg_E2", "var_neg_E"]].fillna(0.0)
    by_key = df.groupby(by=KEYS)
    n_neg_total = by_key["n_neg"].transform("sum")
    mean_neg_total = by_key["sum_neg_E"].transform("sum") / n_neg_total
    with np.errstate(invalid="ignore", divide="ignore"):
        shift = (df["sum_neg_E"] / df["n_neg"] - mean_neg_total) ** 2
    # Chan et al.: pooled M2 = sum of within-batch M2 plus between-batch spread around the pooled mean
    df["m2_neg"] = df["n_neg"] * df["var_neg_E"] + df["n_neg"] * shift.where(df["n_neg"] > 0, 0.0)
    return df.groupby(by=KEYS).agg(
        n=("n", "sum"),
        sum_C=("sum_C", "sum"),
        max_D=("max_D", "max"),
        n_neg=("n_neg", "sum"),
        sum_neg_E=("sum_neg_E", "sum"),
        sum_neg_E2=("sum_neg_E2", "sum"),
        m2_neg=("m2_neg", "sum"),
    )


def conditional_py_st_pa_grp_batches(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    partials: list[pa.Table] = []
    for batch in pq.ParquetFile(data_set.data.source_file_path_parquet).iter_batches(
            batch_size=TARGET_ROWS_PER_PARTITION,
            columns=["grp", "subgrp", "C", "D", "E"],
    ):
        table = pa.Table.from_batches([batch])
        E = table["E"]
        # nulls are skipped by the aggregations, which turns "E < 0" into a per-row condition
        neg_E = pc_if_else(pc_less(E, 0), E, pa.scalar(None, pa.float64()))
        table = table.append_column("negE", neg_E).append_column("negE2", pc_multiply(neg_E, neg_E))
        partials.append(table.group_by(KEYS).aggregate([
            ("E", "count"),
            ("C", "sum"),
            ("D", "max"),
            ("negE", "count"),
            ("negE", "sum"),
            ("negE2", "sum"),
            ("negE", "variance", pc.VarianceOptions(ddof=0)),
        ]))
    merged = merge_partials(partials)
    n_neg = merged["n_neg"].where(merged["n_neg"] > 0)  # NaN for groups without negative E
    result_df = pd.DataFrame({
        "mean_of_C": merged["sum_C"] / merged["n"],
        "max_of_D": merged["max_D"],
        "cond_var_of_E": merged["m2_neg"] / n_neg,
        "cond_var_of_E2": merged["sum_neg_E2"] / n_neg - (merged["sum_neg_E"] / n_neg) ** 2,
    }).reset_index()
    return result_df
