#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.challenges.conditional.conditional_reporter
from typing import NamedTuple

from spark_agg_methods_common_python.perf_test_common import Challenge, SummarizedPerformanceOfMethodAtDataSize

from challenges.conditional.conditional_record_runs_pyspark import ConditionalPysparkPersistedRunResultLog
from challenges.conditional.conditional_strategy_directory_pyspark import CONDITIONAL_STRATEGIES_USING_PYSPARK_REGISTRY
from src.challenges.conditional.conditional_record_runs import regressor_from_run_result

CHALLENGE = Challenge.CONDITIONAL
EXPECTED_SIZES = [3 * 3 * 10**x for x in range(1, 5 + 2)]
FINAL_REPORT_FILE_PATH = 'results/cond_results_intermediate.csv'


class CondResult(NamedTuple):
    name: str
    interface: str
    b0: float
    b0_low: float
    b0_high: float
    b1: float
    b1_low: float
    b1_high: float
    s2: float
    s2_low: float
    s2_high: float


def analyze_run_results():
    summary_status: list[SummarizedPerformanceOfMethodAtDataSize] = []
    challenge_method_list = CONDITIONAL_STRATEGIES_USING_PYSPARK_REGISTRY
    reader = ConditionalPysparkPersistedRunResultLog()
    raw_test_runs = reader.read_run_result_file()
    structured_test_results = reader.structure_test_results(
        challenge_method_list=challenge_method_list,
        expected_sizes=EXPECTED_SIZES,
        regressor_from_run_result=regressor_from_run_result,
        test_runs=raw_test_runs,
    )
    summary_status.extend(
        reader.do_regression(
            challenge=CHALLENGE,
            challenge_method_list=challenge_method_list,
            test_results_by_strategy_name_by_data_size=structured_test_results
        )
    )
    reader.print_summary(summary_status, FINAL_REPORT_FILE_PATH)


if __name__ == "__main__":
    print(f"Running {__file__}")
    analyze_run_results()
    print("Done!")
