from spark_agg_methods_common_python.challenges.six_field_test_data.six_domain_logic.merging_samples import (
    calculate_solutions_progressively,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import Challenge

from src.challenges.six_field_test_data.six_test_data_for_py_st import (
    SixDataSetPythonST, TChallengePythonSTAnswer,
)

CHALLENGE = Challenge.VANILLA


def vanilla_py_st_pd_prog_numpy(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    df_result = calculate_solutions_progressively(
        data_size=data_set.data_description,
        challenges=[CHALLENGE],
    )[CHALLENGE]
    return df_result
