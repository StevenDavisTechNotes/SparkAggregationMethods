from typing import cast

import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_py_only import (
    SixDataSetPythonOnly, TChallengePythonOnlyAnswer,
)


def bi_level_py_only_pd_grp_numpy(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonOnly,
) -> TChallengePythonOnlyAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible"
    df = pd.read_parquet(data_set.data.source_file_path_parquet, engine='pyarrow')
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
