from typing import cast

import numpy
import pandas as pd

from src.six_field_test_data.six_generate_test_data import DataSetPythonOnly, TChallengePythonOnlyAnswer
from src.six_field_test_data.six_test_data_types import ExecutionParameters


def bi_level_py_only_pd_grp_numpy(
        exec_params: ExecutionParameters,
        data_set: DataSetPythonOnly,
) -> TChallengePythonOnlyAnswer:
    if data_set.description.num_data_points > 9 * 10**6:
        return "infeasible"
    df = data_set.data.df_src
    df_result = (
        df
        .groupby(by=["grp"])
        .apply(inner_agg_method)
        .sort_values(by=["grp"])
        .reset_index(drop=False)
    )
    return df_result


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.Series:
    C = dfPartition['C']
    D = dfPartition['D']
    sub_group_e = dfPartition.groupby('subgrp')['E']
    return pd.Series({
        "mean_of_C": numpy.mean(C),
        "max_of_D": numpy.max(D),
        "avg_var_of_E": sub_group_e.var(ddof=0).mean(),
        "avg_var_of_E2": cast(
            pd.Series,
            sub_group_e
            .agg(lambda col_e:
                 ((col_e * col_e).sum() / col_e.count() -
                  (col_e.sum() / col_e.count())**2))
        ).mean(),
    }, dtype=float)
