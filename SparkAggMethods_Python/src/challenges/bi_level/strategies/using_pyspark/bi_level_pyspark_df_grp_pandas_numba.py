import numpy as np
import pandas as pd

from challenges.bi_level.bi_level_test_data_types import (postAggSchema,
                                                          result_columns)
from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.tidy_spark_session import TidySparkSession

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
    def my_looplift_var(E: np.ndarray) -> float:
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
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> PysparkPythonPendingAnswerSet:
    if numba is None:
        return PysparkPythonPendingAnswerSet(
            feasible=False)
    df = data_set.data.dfSrc
    df = (
        df
        .groupBy(df.grp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    df = df.orderBy(df.grp)
    return PysparkPythonPendingAnswerSet(spark_df=df)


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    C = np.array(dfPartition['C'])
    D = np.array(dfPartition['D'])
    subgroupedE = dfPartition.groupby('subgrp')['E']
    return pd.DataFrame([[
        group_key,
        my_numba_mean(C),
        my_numba_max(D),
        subgroupedE.apply(lambda x: my_numba_var(np.array(x))).mean(),
        subgroupedE.apply(lambda x: my_looplift_var(np.array(x))).mean(),
    ]], columns=result_columns)
