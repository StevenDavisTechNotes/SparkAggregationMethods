#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.challenges.bi_level.bi_level_reporter

from six_field_test_data.six_test_data_types import Challenge
from src.challenges.bi_level.bi_level_record_runs import (
    EXPECTED_SIZES, FINAL_REPORT_FILE_PATH, BiLevelPersistedRunResult, BiLevelPersistedRunResultLog,
    regressor_from_run_result,
)
from src.challenges.bi_level.bi_level_strategy_directory import (
    STRATEGIES_USING_DASK_REGISTRY, STRATEGIES_USING_PYSPARK_REGISTRY, STRATEGIES_USING_PYTHON_ONLY_REGISTRY,
)
from src.perf_test_common import CalcEngine, PersistedRunResultBase
from src.six_field_test_data.six_runner_base import SummarizedPerformanceOfMethodAtDataSize
from src.six_field_test_data.size_reporter_base import do_regression, print_summary, structure_test_results

CHALLENGE = Challenge.BI_LEVEL


def persisted_run_result_qualifier(
        result: PersistedRunResultBase,
) -> bool:
    assert isinstance(result, BiLevelPersistedRunResult)
    return True  # result.record_count == 9


def analyze_run_results():
    summary_status: list[SummarizedPerformanceOfMethodAtDataSize] = []
    for engine in CalcEngine:
        challenge_method_list = (
            STRATEGIES_USING_DASK_REGISTRY
            if engine == CalcEngine.DASK else
            STRATEGIES_USING_PYSPARK_REGISTRY
            if engine == CalcEngine.PYSPARK else
            STRATEGIES_USING_PYTHON_ONLY_REGISTRY
            if engine == CalcEngine.PYTHON_ONLY else
            []
        )
        reader = BiLevelPersistedRunResultLog(engine)
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


if __name__ == "__main__":
    analyze_run_results()
    print("Done!")
