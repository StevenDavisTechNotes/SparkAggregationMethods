#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.challenges.vanilla.vanilla_reporter

from src.challenges.vanilla.vanilla_record_runs import (
    EXPECTED_SIZES, FINAL_REPORT_FILE_PATH, VanillaPersistedRunResult, VanillaPersistedRunResultLog,
)
from src.challenges.vanilla.vanilla_strategy_directory import (
    STRATEGIES_USING_DASK_REGISTRY, STRATEGIES_USING_PYSPARK_REGISTRY, STRATEGIES_USING_PYTHON_ONLY_REGISTRY,
    STRATEGIES_USING_SCALA_REGISTRY,
)
from src.perf_test_common import CalcEngine, PersistedRunResultBase
from src.six_field_test_data.six_runner_base import SummarizedPerformanceOfMethodAtDataSize
from src.six_field_test_data.six_test_data_types import Challenge
from src.six_field_test_data.size_reporter_base import do_regression, print_summary, structure_test_results

CHALLENGE = Challenge.VANILLA


def regressor_from_run_result(
        result: PersistedRunResultBase,
) -> int:
    assert isinstance(result, VanillaPersistedRunResult)
    return result.num_data_points


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
            STRATEGIES_USING_SCALA_REGISTRY
            if engine == CalcEngine.SCALA_SPARK else
            []
        )
        reader = VanillaPersistedRunResultLog(engine)
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


def analyze_run_results_new():
    summary_status: list[SummarizedPerformanceOfMethodAtDataSize] = []
    for engine in CalcEngine:
        challenge_method_list = (
            STRATEGIES_USING_DASK_REGISTRY
            if engine == CalcEngine.DASK else
            STRATEGIES_USING_PYSPARK_REGISTRY
            if engine == CalcEngine.PYSPARK else
            STRATEGIES_USING_PYTHON_ONLY_REGISTRY
            if engine == CalcEngine.PYTHON_ONLY else
            STRATEGIES_USING_SCALA_REGISTRY
            if engine == CalcEngine.SCALA_SPARK else
            []
        )
        reader = VanillaPersistedRunResultLog(engine)
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
    # analyze_run_results_new()
    print("Done!")
