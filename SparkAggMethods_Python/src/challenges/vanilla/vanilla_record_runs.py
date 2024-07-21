import os
from dataclasses import dataclass

from src.perf_test_common import CalcEngine
from src.utils.utils import root_folder_abs_path

PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/vanilla_dask_runs.csv'
PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/vanilla_pyspark_runs.csv'
PYTHON_ONLY_RUN_LOG_FILE_PATH = 'results/vanilla_python_only_runs.csv'
SCALA_RUN_LOG_FILE_PATH = '../results/Scala/vanilla_runs_scala.csv'
FINAL_REPORT_FILE_PATH = 'results/vanilla_results.csv'
EXPECTED_SIZES = [3 * 3 * 10**x for x in range(0, 6 + 1)]


@dataclass(frozen=True)
class PersistedRunResult:
    strategy_name: str
    language: str
    engine: CalcEngine
    strategy_w_language_name: str
    interface: str
    data_size: int
    elapsed_time: float
    record_count: int


def derive_run_log_file_path(
        engine: CalcEngine,
) -> str:
    match engine:
        case CalcEngine.DASK:
            run_log = PYTHON_DASK_RUN_LOG_FILE_PATH
        case CalcEngine.PYSPARK:
            run_log = PYTHON_PYSPARK_RUN_LOG_FILE_PATH
        case CalcEngine.PYTHON_ONLY:
            run_log = PYTHON_ONLY_RUN_LOG_FILE_PATH
        case CalcEngine.SCALA_SPARK:
            run_log = SCALA_RUN_LOG_FILE_PATH
        case _:
            raise ValueError(f"Unknown engine: {engine}")
    return os.path.join(
        root_folder_abs_path(),
        run_log)


def engine_implied_language(
        engine: CalcEngine,
) -> str:
    match engine:
        case CalcEngine.DASK | CalcEngine.PYSPARK | CalcEngine.PYTHON_ONLY:
            return "python"
        case CalcEngine.SCALA_SPARK:
            return "scala"
        case _:
            raise ValueError(f"Unknown engine: {engine}")


def regressor_from_run_result(
        result: PersistedRunResult,
) -> int:
    return result.data_size
