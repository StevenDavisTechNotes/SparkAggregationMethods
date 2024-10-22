#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.challenges.bi_level.bi_level_reporter
from spark_agg_methods_common_python.challenges.bi_level.bi_level_record_runs import (
    BiLevelPersistedRunResult, BiLevelPersistedRunResultLog, regressor_from_run_result,
)
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, Challenge, PersistedRunResultBase, SummarizedPerformanceOfMethodAtDataSize,
)

from src.challenges.bi_level.bi_level_record_runs_pyspark import BiLevelPysparkPersistedRunResultLog
from src.challenges.bi_level.bi_level_strategy_directory_pyspark import BI_LEVEL_STRATEGIES_USING_PYSPARK_REGISTRY

CHALLENGE = Challenge.BI_LEVEL
EXPECTED_SIZES = [1, 10, 100, 1000]
FINAL_REPORT_FILE_PATH = 'results/bilevel_results_intermediate.csv'


def persisted_run_result_qualifier(
        result: PersistedRunResultBase,
) -> bool:
    assert isinstance(result, BiLevelPersistedRunResult)
    return True  # result.num_output_rows == 9


def analyze_run_results():
    summary_status: list[SummarizedPerformanceOfMethodAtDataSize] = []
    for engine in CalcEngine:
        challenge_method_list = (
            # BI_LEVEL_STRATEGIES_USING_DASK_REGISTRY if engine == CalcEngine.DASK else
            BI_LEVEL_STRATEGIES_USING_PYSPARK_REGISTRY if engine == CalcEngine.PYSPARK else
            # BI_LEVEL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY if engine == CalcEngine.PYTHON_ONLY else
            []
        )
        reader = BiLevelPysparkPersistedRunResultLog()
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
    BiLevelPersistedRunResultLog.print_summary(summary_status, FINAL_REPORT_FILE_PATH)


if __name__ == "__main__":
    print(f"Running {__file__}")
    analyze_run_results()
    print("Done!")
