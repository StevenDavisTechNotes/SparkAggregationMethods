import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_py_only import (
    SixDataSetPythonOnly, TChallengePythonOnlyAnswer,
)


def vanilla_py_only_pd_grp_numpy(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonOnly,
) -> TChallengePythonOnlyAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible"
    df = df = pd.read_parquet(data_set.data.source_file_path_parquet, engine='pyarrow')
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
