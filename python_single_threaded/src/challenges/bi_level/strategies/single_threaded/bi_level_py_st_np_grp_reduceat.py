import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer

# Strategy by Claude


def bi_level_py_st_np_grp_reduceat(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"
    table = pq.read_table(
        data_set.data.source_file_path_parquet,
        columns=["grp", "subgrp", "C", "D", "E"],
    )
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
    mean_of_E = np.add.reduceat(E, starts) / n
    # two-pass (centered) variance avoids cancellation in sum(E**2) / n - mean**2
    deviation = E - np.repeat(mean_of_E, n)
    var_of_E = np.add.reduceat(deviation * deviation, starts) / n
    var_of_E2 = np.add.reduceat(E * E, starts) / n - mean_of_E ** 2
    sum_of_C = np.add.reduceat(C, starts)
    max_of_D = np.maximum.reduceat(D, starts)
    # `present` is ascending, so each grp is one contiguous run of subgroups
    grp_of_key = present // stride
    first_of_grp = np.flatnonzero(np.diff(grp_of_key, prepend=-1))
    num_subgroups = np.diff(
        first_of_grp,
        append=len(present),
    )
    df_result = pd.DataFrame({
        "grp": grp_of_key[first_of_grp],
        "mean_of_C": np.add.reduceat(
            sum_of_C,
            first_of_grp,
        )
        / np.add.reduceat(n, first_of_grp),
        "max_of_D": np.maximum.reduceat(
            max_of_D,
            first_of_grp,
        ),

        "avg_var_of_E": np.add.reduceat(
            var_of_E,
            first_of_grp,
        )
        / num_subgroups,
        "avg_var_of_E2": np.add.reduceat(
            var_of_E2,
            first_of_grp,
        )
        / num_subgroups,
    })
    return df_result
