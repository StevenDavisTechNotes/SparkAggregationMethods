#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.challenges.conditional.conditional_reporter
from typing import NamedTuple

from src.challenges.conditional.conditional_record_runs import (
    EXPECTED_SIZES, FINAL_REPORT_FILE_PATH, ConditionalPersistedRunResultLog, regressor_from_run_result,
)
from src.challenges.conditional.conditional_strategy_directory import STRATEGIES_USING_PYSPARK_REGISTRY
from src.perf_test_common import CalcEngine, SummarizedPerformanceOfMethodAtDataSize
from src.six_field_test_data.six_test_data_types import Challenge

CHALLENGE = Challenge.CONDITIONAL


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
    engine = CalcEngine.PYSPARK
    challenge_method_list = STRATEGIES_USING_PYSPARK_REGISTRY
    reader = ConditionalPersistedRunResultLog(engine)
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
            engine=engine,
            challenge_method_list=challenge_method_list,
            test_results_by_strategy_name_by_data_size=structured_test_results
        )
    )
    reader.print_summary(summary_status, FINAL_REPORT_FILE_PATH)


if __name__ == "__main__":
    print(f"Running {__file__}")
    analyze_run_results()
    print("Done!")
