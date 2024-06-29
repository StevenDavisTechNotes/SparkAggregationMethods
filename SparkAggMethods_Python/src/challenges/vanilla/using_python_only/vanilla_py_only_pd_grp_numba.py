# cSpell: ignore nopython, prange
import numpy
import pandas as pd

from challenges.vanilla.vanilla_test_data_types import result_columns
from six_field_test_data.six_generate_test_data import (
    DataSetPythonOnly, TChallengePythonOnlyAnswer)
from six_field_test_data.six_test_data_types import ExecutionParameters

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
        return accE2 / n - (accE / n)**2  # pyright: ignore

except ImportError:
    numba = None


def vanilla_py_only_pd_grp_numba(
        exec_params: ExecutionParameters,
        data_set: DataSetPythonOnly,
) -> TChallengePythonOnlyAnswer:
    if numba is None:
        return "infeasible"

    # df = data_set.data.dfSrc
    # df = (
    #     df.groupby(df.grp, df.subgrp)
    #     .applyInPandas(inner_agg_method, pyspark_post_agg_schema)
    #     .orderBy(df.grp, df.subgrp)
    # )
    return pd.DataFrame()


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    subgroup_key = dfPartition['subgrp'].iloc[0]
    C = numpy.array(dfPartition['C'])
    D = numpy.array(dfPartition['D'])
    E = numpy.array(dfPartition['E'])
    return pd.DataFrame([[
        group_key,
        subgroup_key,
        my_numba_mean(C),
        my_numba_max(D),
        my_numba_var(E),
        my_loop_lift_var(E),
    ]], columns=result_columns)
