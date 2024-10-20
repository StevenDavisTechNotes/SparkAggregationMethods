#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.challenges.vanilla.vanilla_reporter

from spark_agg_methods_common_python.challenges.vanilla.vanilla_record_runs import (
    VanillaPersistedRunResultLog, regressor_from_run_result,
)
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, Challenge, SummarizedPerformanceOfMethodAtDataSize,
)

from src.challenges.vanilla.vanilla_record_runs_pyspark import VanillaPysparkPersistedRunResultLog
from src.challenges.vanilla.vanilla_strategy_directory_pyspark import STRATEGIES_USING_PYSPARK_REGISTRY
from src.challenges.vanilla.vanilla_strategy_directory_scala_spark import STRATEGIES_USING_SCALA_REGISTRY

CHALLENGE = Challenge.VANILLA
FINAL_REPORT_FILE_PATH = 'results/vanilla_results_intermediate.csv'
EXPECTED_SIZES = [3 * 3 * 10**x for x in range(0, 6 + 1)]


def analyze_run_results():
    summary_status: list[SummarizedPerformanceOfMethodAtDataSize] = []
    for engine in CalcEngine:
        challenge_method_list = (
            # STRATEGIES_USING_DASK_REGISTRY if engine == CalcEngine.DASK else
            STRATEGIES_USING_PYSPARK_REGISTRY if engine == CalcEngine.PYSPARK else
            # STRATEGIES_USING_PYTHON_ONLY_REGISTRY if engine == CalcEngine.PYTHON_ONLY else
            STRATEGIES_USING_SCALA_REGISTRY if engine == CalcEngine.SCALA_SPARK else
            []
        )
        reader = VanillaPysparkPersistedRunResultLog()
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
    VanillaPersistedRunResultLog.print_summary(summary_status, FINAL_REPORT_FILE_PATH)


if __name__ == "__main__":
    print(f"Running {__file__}")
    analyze_run_results()
    print("Done!")
