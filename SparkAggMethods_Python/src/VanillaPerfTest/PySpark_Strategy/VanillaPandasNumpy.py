import numpy
import pandas as pd

from SixFieldCommon.PySpark_SixFieldTestData import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.TidySparkSession import TidySparkSession
from VanillaPerfTest.VanillaDataTypes import (pyspark_post_agg_schema,
                                              result_columns)


def vanilla_pandas_numpy(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> PysparkPythonPendingAnswerSet:

    df = data_set.data.dfSrc
    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, pyspark_post_agg_schema)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return PysparkPythonPendingAnswerSet(spark_df=df)


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    subgroup_key = dfPartition['subgrp'].iloc[0]
    C = dfPartition['C']
    D = dfPartition['D']
    E = dfPartition['E']
    return pd.DataFrame([[
        group_key,
        subgroup_key,
        numpy.mean(C),
        numpy.max(D),
        numpy.var(E),
        numpy.inner(E, E) / E.count()
        - (numpy.sum(E) / E.count())**2,  # type: ignore
    ]], columns=result_columns)
