import numpy
import pandas as pd

from src.six_field_test_data.six_generate_test_data import DataSetPythonOnly, TChallengePythonOnlyAnswer
from src.six_field_test_data.six_test_data_types import SixTestExecutionParameters


def vanilla_py_only_pd_grp_numpy(
        exec_params: SixTestExecutionParameters,
        data_set: DataSetPythonOnly,
) -> TChallengePythonOnlyAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible"
    df = data_set.data.df_src
    df_result = (
        df
        .groupby(by=["grp", "subgrp"])
        .apply(inner_agg_method)
        .sort_values(by=["grp", "subgrp"])
        .reset_index(drop=False)
    )
    return df_result


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.Series:
    C = dfPartition['C']
    D = dfPartition['D']
    E = dfPartition['E']
    return pd.Series({
        "mean_of_C": numpy.mean(C),
        "max_of_D": numpy.max(D),
        "var_of_E": numpy.var(E),
        "var_of_E2": numpy.inner(E, E) / E.count() - (numpy.sum(E) / E.count())**2,
    }, dtype=float)
