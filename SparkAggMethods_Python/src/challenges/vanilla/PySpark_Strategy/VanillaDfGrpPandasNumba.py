import numpy
import pandas as pd
from numba import float64 as numba_float64
from numba import jit, prange

from challenges.vanilla.VanillaDataTypes import (pyspark_post_agg_schema,
                                                 result_columns)
from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.TidySparkSession import TidySparkSession


def vanilla_df_grp_pandas_numba(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> PysparkPythonPendingAnswerSet:
    df = data_set.data.dfSrc

    df = (
        df.groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, pyspark_post_agg_schema)
        .orderBy(df.grp, df.subgrp)
    )
    return PysparkPythonPendingAnswerSet(spark_df=df)


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_mean(C: numpy.ndarray) -> numpy.float64:
    return numpy.mean(C)


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_max(C: numpy.ndarray) -> numpy.float64:
    return numpy.max(C)


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_var(C: numpy.ndarray) -> numpy.float64:
    return numpy.var(C)


@jit(numba_float64(numba_float64[:]), parallel=True, nopython=True)
def my_looplift_var(E: numpy.ndarray) -> float:
    n = len(E)
    accE2 = 0.
    for i in prange(n):
        accE2 += E[i] ** 2
    accE = 0.
    for i in prange(n):
        accE += E[i]
    return accE2 / n - (accE / n)**2  # pyright: ignore


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
        my_looplift_var(E),
    ]], columns=result_columns)
