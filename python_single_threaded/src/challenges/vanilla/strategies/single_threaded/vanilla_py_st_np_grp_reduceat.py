import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer

# Strategy by Claude


def vanilla_py_st_np_grp_reduceat(
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
    mean_of_E = np.add.reduceat(E, starts) / n
    # two-pass (centered) variance avoids cancellation in sum(E**2) / n - mean**2
    deviation = E - np.repeat(mean_of_E, n)
    df_result = pd.DataFrame({
        "grp": present // stride,
        "subgrp": present % stride,
        "mean_of_C": np.add.reduceat(C, starts) / n,
        "max_of_D": np.maximum.reduceat(D, starts),
        "var_of_E": np.add.reduceat(deviation * deviation, starts) / n,
        "var_of_E2": np.add.reduceat(E * E, starts) / n - mean_of_E ** 2,
    })
    return df_result
