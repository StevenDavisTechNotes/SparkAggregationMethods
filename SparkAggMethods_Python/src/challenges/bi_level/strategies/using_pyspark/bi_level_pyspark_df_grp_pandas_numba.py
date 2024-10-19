# cSpell: ignore nopython, prange
import numpy as np
import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.bi_level.bi_level_test_data_types import BI_LEVEL_RESULT_COLUMNS, postAggSchema
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_spark_session import TidySparkSession

try:
    import numba  # pyright: ignore[reportMissingImports]

    @numba.jit(numba.float64(numba.float64[:]), nopython=True)
    def my_numba_mean(C: np.ndarray) -> np.float64:
        return np.mean(C)

    @numba.jit(numba.float64(numba.float64[:]), nopython=True)
    def my_numba_max(D: np.ndarray) -> np.float64:
        return np.max(D)

    @numba.jit(numba.float64(numba.float64[:]), nopython=True)
    def my_numba_var(E: np.ndarray) -> np.float64:
        return np.var(E)

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


def bi_level_pyspark_df_grp_pandas_numba(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if numba is None:
        return "infeasible"
    df = data_set.data.df_src
    df = (
        df
        .groupBy(df.grp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    df = df.orderBy(df.grp)
    return df


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    C = np.array(dfPartition['C'])
    D = np.array(dfPartition['D'])
    sub_group_E = dfPartition.groupby('subgrp')['E']
    return pd.DataFrame([[
        group_key,
        my_numba_mean(C),
        my_numba_max(D),
        sub_group_E.apply(lambda x: my_numba_var(np.array(x))).mean(),
        sub_group_E.apply(lambda x: my_loop_lift_var(np.array(x))).mean(),
    ]], columns=BI_LEVEL_RESULT_COLUMNS)
