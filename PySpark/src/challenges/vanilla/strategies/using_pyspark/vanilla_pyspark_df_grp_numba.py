# cSpell: ignore nopython, prange
import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    VANILLA_RESULT_COLUMNS,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.challenges.vanilla.vanilla_test_data_types_pyspark import (
    pyspark_post_agg_schema,
)
from src.utils.tidy_session_pyspark import TidySparkSession

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


def vanilla_pyspark_df_grp_numba(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if numba is None:
        return "infeasible", "Requires Numba be installed"
    df = data_set.data.open_source_data_as_df(spark_session)
    df = (
        df.groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, pyspark_post_agg_schema)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return df


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
    ]], columns=VANILLA_RESULT_COLUMNS)
