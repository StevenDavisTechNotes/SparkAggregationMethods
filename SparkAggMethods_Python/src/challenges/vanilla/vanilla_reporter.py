#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.challenges.vanilla.vanilla_reporter

from src.challenges.vanilla.vanilla_record_runs import (
    EXPECTED_SIZES, FINAL_REPORT_FILE_PATH, VanillaPersistedRunResultLog, regressor_from_run_result,
)
from src.challenges.vanilla.vanilla_strategy_directory import (
    STRATEGIES_USING_DASK_REGISTRY, STRATEGIES_USING_PYSPARK_REGISTRY, STRATEGIES_USING_PYTHON_ONLY_REGISTRY,
    STRATEGIES_USING_SCALA_REGISTRY,
)
from src.perf_test_common import CalcEngine, SummarizedPerformanceOfMethodAtDataSize
from src.six_field_test_data.six_test_data_types import Challenge

CHALLENGE = Challenge.VANILLA


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
    VanillaPersistedRunResultLog.print_summary(summary_status, FINAL_REPORT_FILE_PATH)


if __name__ == "__main__":
    print(f"Running {__file__}")
    analyze_run_results()
    print("Done!")
