import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer

# Strategy by Claude


def cond_py_st_np_grp_reduceat(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"
    table = pq.read_table(data_set.data.source_file_path_parquet, columns=["grp", "subgrp", "C", "D", "E"])
    grp = table["grp"].to_numpy()
    subgrp = table["subgrp"].to_numpy()
    # dense composite key; sorting by it makes every (grp, subgrp) a contiguous run, in answer order
    stride = int(subgrp.max()) + 1
    key = grp.astype(np.int64) * stride + subgrp
    order = np.argsort(key, kind="stable")
    n_per_key = np.bincount(key)
    present = np.flatnonzero(n_per_key)
    n = n_per_key[present]
    starts = np.cumsum(n) - n
    C = table["C"].to_numpy()[order]
    D = table["D"].to_numpy()[order]
    E = table["E"].to_numpy()[order]
    is_neg = E < 0
    E_neg = E[is_neg]  # still grouped by key, in key order
    n_neg = np.add.reduceat(
        is_neg.astype(np.int64), starts)
    # reduceat misreads empty segments, so only groups with negative E are reduced; the rest stay NaN
    has_neg = n_neg > 0
    n_neg = n_neg[has_neg]
    starts_neg = np.cumsum(n_neg) - n_neg
    mean_of_E_neg = np.add.reduceat(E_neg, starts_neg) / n_neg
    deviation = E_neg - np.repeat(mean_of_E_neg, n_neg)
    cond_var_of_E = np.full(
        len(present), np.nan)
    cond_var_of_E[has_neg] = np.add.reduceat(
        deviation * deviation, starts_neg) / n_neg
    cond_var_of_E2 = np.full(
        len(present), np.nan)
    cond_var_of_E2[has_neg] = np.add.reduceat(
        E_neg * E_neg, starts_neg) / n_neg - mean_of_E_neg ** 2
    df_result = pd.DataFrame({
        "grp": present // stride,
        "subgrp": present % stride,
        "mean_of_C": np.add.reduceat(C, starts) / n,
        "max_of_D": np.maximum.reduceat(D, starts),
        "cond_var_of_E": cond_var_of_E,
        "cond_var_of_E2": cond_var_of_E2,
    })
    return df_result
