import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def cond_py_st_pd_grp_agg(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"
    source_df = pd.read_parquet(
        data_set.data.source_file_path_parquet,
        columns=["grp", "subgrp", "C", "D", "E"],
        engine="pyarrow",
    )
    neg_e_df = source_df.loc[source_df["E"] < 0, ["grp", "subgrp", "E"]]
    neg_e_df["E2"] = neg_e_df["E"] * neg_e_df["E"]
    grouped = source_df.groupby(by=["grp", "subgrp"])
    grouped_neg = neg_e_df.groupby(by=["grp", "subgrp"])
    intermediate_df = pd.DataFrame({
        "mean_of_C": grouped["C"].mean(),
        "max_of_D": grouped["D"].max(),
    })
    # Series assignment aligns on (grp, subgrp), so groups without any negative E get NaN
    intermediate_df["cond_var_of_E"] = grouped_neg["E"].var(ddof=0)
    intermediate_df["cond_var_of_E2"] = (
        grouped_neg["E2"].mean()
        - grouped_neg["E"].mean() ** 2
    )
    return intermediate_df.reset_index()
