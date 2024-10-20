# cSpell: ignore nopython, prange
import numpy as np
import pandas as pd
from spark_agg_methods_common_python.challenges.conditional.conditional_test_data_types import (
    AGGREGATION_COLUMNS_4, GROUP_BY_COLUMNS,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.conditional.conditional_test_data_types_pyspark import POST_AGGREGATION_SCHEMA_4
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession

try:
    import numba  # pyright: ignore[reportMissingImports]

    @numba.jit(numba.float64(numba.float64[:]), nopython=True)
    def my_numba_mean(C: np.ndarray) -> np.float64:
        return np.mean(C)

    @numba.jit(numba.float64(numba.float64[:]), nopython=True)
    def my_numba_max(C: np.ndarray) -> np.float64:
        return np.max(C)

    @numba.jit(numba.float64(numba.float64[:]), nopython=True)
    def my_numba_var(C: np.ndarray) -> np.float64:
        return np.var(C)

    @numba.jit(numba.float64(numba.float64[:]), parallel=True, nopython=True)
    def my_loop_lift_var(E: np.ndarray) -> float:
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


def cond_pyspark_df_grp_pandas_numba(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    df = data_set.data.df_src
    if numba is None:
        return "infeasible"

    df = (
        df
        .groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, POST_AGGREGATION_SCHEMA_4)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return df


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    subgroup_key = dfPartition['subgrp'].iloc[0]
    C = np.array(dfPartition['C'])
    D = np.array(dfPartition['D'])
    posE = np.array(dfPartition[dfPartition.E < 0]['E'])
    return pd.DataFrame([[
        group_key,
        subgroup_key,
        my_numba_mean(C),
        my_numba_max(D),
        my_numba_var(posE),
        my_loop_lift_var(posE),
    ]], columns=GROUP_BY_COLUMNS + AGGREGATION_COLUMNS_4)
