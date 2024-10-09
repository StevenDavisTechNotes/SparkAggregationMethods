#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.challenges.conditional.conditional_reporter
from typing import NamedTuple

from six_field_test_data.six_runner_base import SummarizedPerformanceOfMethodAtDataSize
from six_field_test_data.six_test_data_types import Challenge
from src.challenges.conditional.conditional_record_runs import (
    EXPECTED_SIZES, FINAL_REPORT_FILE_PATH, ConditionalPersistedRunResultLog, regressor_from_run_result,
)
from src.challenges.conditional.conditional_strategy_directory import STRATEGIES_USING_PYSPARK_REGISTRY
from src.perf_test_common import CalcEngine
from src.six_field_test_data.size_reporter_base import do_regression, print_summary, structure_test_results

TEMP_RESULT_FILE_PATH = "d:/temp/SparkPerfTesting/temp.csv"
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
    structured_test_results = structure_test_results(
        challenge_method_list=challenge_method_list,
        expected_sizes=EXPECTED_SIZES,
        regressor_from_run_result=regressor_from_run_result,
        test_runs=raw_test_runs,
    )
    summary_status.extend(
        do_regression(
            CHALLENGE,
            engine,
            challenge_method_list,
            structured_test_results
        )
    )
    print_summary(summary_status, FINAL_REPORT_FILE_PATH)
