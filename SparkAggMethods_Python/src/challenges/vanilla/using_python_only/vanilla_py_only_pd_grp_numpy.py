import numpy
import pandas as pd

from challenges.vanilla.vanilla_test_data_types import result_columns
from six_field_test_data.six_generate_test_data import (
    DataSetPythonOnly, TChallengePythonOnlyAnswer)
from six_field_test_data.six_test_data_types import ExecutionParameters


def vanilla_py_only_pd_grp_numpy(
        exec_params: ExecutionParameters,
        data_set: DataSetPythonOnly,
) -> TChallengePythonOnlyAnswer:

    # df = data_set.data.dfSrc
    # df = (
    #     df
    #     .groupBy(df.grp, df.subgrp)
    #     .applyInPandas(inner_agg_method, pyspark_post_agg_schema)
    # )
    # df = df.orderBy(df.grp, df.subgrp)
    return pd.DataFrame()


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
