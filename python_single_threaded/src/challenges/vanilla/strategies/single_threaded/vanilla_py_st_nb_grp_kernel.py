# cSpell: ignore njit

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import TARGET_ROWS_PER_PARTITION
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer

# Strategy by Claude

# accumulator fields, one row per (grp, subgrp) key
N, SUM_C, MAX_D, SUM_E, SUM_E2, MEAN_E, M2_E, COMP_E, COMP_E2 = range(9)
NUM_FIELDS = 9

try:
    import numba

    _INT32_IN = numba.types.Array(numba.int32, 1, "C", readonly=True)  # zero-copy Arrow buffers are read-only
    _FLOAT64_IN = numba.types.Array(numba.float64, 1, "C", readonly=True)
    _SIGNATURE = numba.void(
        _INT32_IN, _INT32_IN, _FLOAT64_IN, _FLOAT64_IN, _FLOAT64_IN, numba.int64, numba.float64[:, ::1],
    )

    @numba.njit
    def add_compensated(acc: np.ndarray, k: int, total_field: int, comp_field: int, x: float) -> None:
        # Neumaier summation keeps sum(E) and sum(E**2) accurate over long runs of rows
        total = acc[k, total_field]
        t = total + x
        if abs(total) >= abs(x):
            acc[k, comp_field] += (total - t) + x
        else:
            acc[k, comp_field] += (x - t) + total
        acc[k, total_field] = t

    @numba.njit(_SIGNATURE)
    def accumulate_batch(
            grp: np.ndarray,
            subgrp: np.ndarray,
            C: np.ndarray,
            D: np.ndarray,
            E: np.ndarray,
            stride: int,
            acc: np.ndarray,
    ) -> None:
        for i in range(grp.shape[0]):
            k = grp[i] * stride + subgrp[i]
            e = E[i]
            n = acc[k, N] + 1.0
            acc[k, N] = n
            acc[k, SUM_C] += C[i]
            if D[i] > acc[k, MAX_D]:
                acc[k, MAX_D] = D[i]
            add_compensated(acc, k, SUM_E, COMP_E, e)
            add_compensated(acc, k, SUM_E2, COMP_E2, e * e)
            # Welford update: numerically stable variance in a single pass
            delta = e - acc[k, MEAN_E]
            acc[k, MEAN_E] += delta / n
            acc[k, M2_E] += delta * (e - acc[k, MEAN_E])

except ImportError:
    numba = None  # ty: ignore[invalid-assignment]


def vanilla_py_st_nb_grp_kernel(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if numba is None:
        return "infeasible", "Needs Numba installed"
    description = data_set.data_description
    stride = description.num_grp_2
    acc = np.zeros((description.num_grp_1 * stride, NUM_FIELDS))
    acc[:, MAX_D] = -np.inf
    for batch in pq.ParquetFile(data_set.data.source_file_path_parquet).iter_batches(
            batch_size=TARGET_ROWS_PER_PARTITION,
            columns=["grp", "subgrp", "C", "D", "E"],
    ):
        accumulate_batch(
            batch.column(0).to_numpy(zero_copy_only=True),
            batch.column(1).to_numpy(zero_copy_only=True),
            batch.column(2).to_numpy(zero_copy_only=True),
            batch.column(3).to_numpy(zero_copy_only=True),
            batch.column(4).to_numpy(zero_copy_only=True),
            stride,
            acc,
        )
    present = np.flatnonzero(acc[:, N])
    a = acc[present]
    n = a[:, N]
    df_result = pd.DataFrame({
        "grp": present // stride,
        "subgrp": present % stride,
        "mean_of_C": a[:, SUM_C] / n,
        "max_of_D": a[:, MAX_D],
        "var_of_E": a[:, M2_E] / n,
        "var_of_E2": (a[:, SUM_E2] + a[:, COMP_E2]) / n - ((a[:, SUM_E] + a[:, COMP_E]) / n) ** 2,
    })
    return df_result
