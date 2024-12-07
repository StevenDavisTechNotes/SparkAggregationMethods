# cSpell: ignore nopython, prange
from typing import cast

import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_py_only import (
    SixDataSetPythonOnly, TChallengePythonOnlyAnswer,
)

try:
    import numba  # pyright: ignore[reportMissingImports]

    @numba.jit(numba.float64(numba.float64[:]), nopython=True)
    def my_numba_mean(C: numpy.ndarray) -> numpy.float64:
        return numpy.mean(C)

    @numba.jit(numba.float64(numba.float64[:]), nopython=True)
    def my_numba_max(C: numpy.ndarray) -> numpy.float64:
        return numpy.max(C)

    @numba.jit(numba.float64(numba.float64[:]), nopython=True)
    def my_numba_var(C: numpy.ndarray) -> numpy.float64:
        return numpy.var(C)

    @numba.jit(numba.float64(numba.float64[:]), parallel=True, nopython=True)
    def my_loop_lift_var(E: numpy.ndarray) -> float:
        assert numba is not None
        n = len(E)
        accE2: float = 0.
        for i in numba.prange(n):
            accE2 += cast(float, E[i]) ** 2
        accE: float = 0.
        for i in numba.prange(n):
            accE += cast(float, E[i])
        return accE2 / n - (accE / n)**2

except ImportError:
    numba = None


def vanilla_py_st_pd_grp_numba(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonOnly,
) -> TChallengePythonOnlyAnswer:
    if numba is None:
        return "infeasible", "Needs Numba installed"
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"

    df = df = pd.read_parquet(data_set.data.source_file_path_parquet, engine='pyarrow')
    df_result = (
        df
        .groupby(by=["grp", "subgrp"])
        .apply(inner_agg_method)
        .sort_values(by=["grp", "subgrp"])
        .reset_index(drop=False)
    )
    return df_result


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.Series:
    C = numpy.array(dfPartition['C'])
    D = numpy.array(dfPartition['D'])
    E = numpy.array(dfPartition['E'])
    return pd.Series({
        "mean_of_C": my_numba_mean(C),
        "max_of_D": my_numba_max(D),
        "var_of_E": my_numba_var(E),
        "var_of_E2": my_loop_lift_var(E),
    }, dtype=float)
