#!python
# usage: python -m src.VanillaPerfTest.VanillaReporter

from src.challenges.vanilla.vanilla_record_runs import (
    EXPECTED_SIZES, FINAL_REPORT_FILE_PATH, PersistedRunResult,
    regressor_from_run_result)
from src.challenges.vanilla.vanilla_strategy_directory import (
    SOLUTIONS_USING_DASK_REGISTRY, SOLUTIONS_USING_PYSPARK_REGISTRY,
    SOLUTIONS_USING_PYTHON_ONLY_REGISTRY, SOLUTIONS_USING_SCALA_REGISTRY)
from src.perf_test_common import CalcEngine
from src.six_field_test_data.six_run_result_types import read_result_file
from src.six_field_test_data.six_runner_base import \
    SummarizedPerformanceOfMethodAtDataSize
from src.six_field_test_data.six_test_data_types import Challenge
from src.six_field_test_data.size_reporter_base import (do_regression,
                                                        print_summary,
                                                        structure_test_results)

CHALLENGE = Challenge.VANILLA


# def make_runs_summary(
#         test_results: dict[str, dict[int, list[PersistedRunResult]]],
# ) -> dict[str, dict[int, int]]:
#     return {strategy_name:
#             {x_variable: len(runs) for x_variable, runs in runs_for_strategy_name.items()}
#             for strategy_name, runs_for_strategy_name in test_results.items()}


def persisted_run_result_qualifier(
        result: PersistedRunResult,
) -> bool:
    return result.record_count == 9


def analyze_run_results():
    summary_status: list[SummarizedPerformanceOfMethodAtDataSize] = []
    for engine in CalcEngine:
        challenge_method_list = (
            SOLUTIONS_USING_DASK_REGISTRY
            if engine == CalcEngine.DASK else
            SOLUTIONS_USING_PYSPARK_REGISTRY
            if engine == CalcEngine.PYSPARK else
            SOLUTIONS_USING_PYTHON_ONLY_REGISTRY
            if engine == CalcEngine.PYTHON_ONLY else
            SOLUTIONS_USING_SCALA_REGISTRY
            if engine == CalcEngine.SCALA_SPARK else
            []
        )
        raw_test_runs = read_result_file(engine, persisted_run_result_qualifier)
        structured_test_results = structure_test_results(
            challenge_method_list=challenge_method_list,
            expected_sizes=EXPECTED_SIZES,
            regressor_from_run_result=regressor_from_run_result,
            test_runs=raw_test_runs,
        )
        summary_status.extend(
            do_regression(
                CHALLENGE,
                challenge_method_list,
                structured_test_results
            )
        )
    print_summary(summary_status, FINAL_REPORT_FILE_PATH)


if __name__ == "__main__":
    analyze_run_results()
    print("Done!")
