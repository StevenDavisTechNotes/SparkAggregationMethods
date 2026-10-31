import polars as pl
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def bi_level_py_st_pl_grp_single(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"
    df = pl.read_parquet(
        source=data_set.data.source_file_path_parquet,
        parallel="row_groups",
        use_pyarrow=True,
    )
    e = pl.col("E")
    group_df = (
        df
        .group_by("grp")
        .agg(
            mean_of_C=pl.col("C").mean(),
            max_of_D=pl.col("D").max(),
        )
    )
    subgroup_df = (
        df
        .group_by("grp", "subgrp")
        .agg(
            var_of_E=e.var(ddof=0),
            var_of_E2=(e * e).sum() / e.count() - (e.sum() / e.count()) ** 2,
        )
        .group_by("grp")
        .agg(
            avg_var_of_E=pl.col("var_of_E").mean(),
            avg_var_of_E2=pl.col("var_of_E2").mean(),
        )
    )
    result_df = (
        group_df
        .join(subgroup_df, on="grp", how="inner")
        .sort("grp")
    )
    return result_df
