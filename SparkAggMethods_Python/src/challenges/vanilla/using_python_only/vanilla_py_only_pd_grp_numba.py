# cSpell: ignore nopython, prange
import numpy
import pandas as pd

from src.six_field_test_data.six_generate_test_data import (
    DataSetPythonOnly, TChallengePythonOnlyAnswer)
from src.six_field_test_data.six_test_data_types import ExecutionParameters

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
        accE2 = 0.
        for i in numba.prange(n):
            accE2 += E[i] ** 2
        accE = 0.
        for i in numba.prange(n):
            accE += E[i]
        return accE2 / n - (accE / n)**2

except ImportError:
    numba = None


def vanilla_py_only_pd_grp_numba(
        exec_params: ExecutionParameters,
        data_set: DataSetPythonOnly,
) -> TChallengePythonOnlyAnswer:
    if numba is None:
        return "infeasible"
    if data_set.data_size.num_data_points > 9 * 10**6:
        return "infeasible"

    df = data_set.data.df_src
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
