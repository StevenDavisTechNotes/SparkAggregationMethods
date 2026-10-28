import polars as pl
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def vanilla_py_st_pd_grp_polars(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"
    df = pl.read_parquet(data_set.data.source_file_path_parquet)
    E = pl.col("E")
    df_result = (
        df
        .group_by("grp", "subgrp")
        .agg(
            mean_of_C=pl.col("C").mean(),
            max_of_D=pl.col("D").max(),
            var_of_E=E.var(ddof=0),
            var_of_E2=(E * E).sum() / E.count() - (E.sum() / E.count()) ** 2,
        )
        .sort("grp", "subgrp")
    )
    return df_result
