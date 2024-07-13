import os
from dataclasses import dataclass

from src.perf_test_common import CalcEngine
from src.utils.utils import root_folder_abs_path

T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/conditional_pyspark_runs.csv'
T_PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/conditional_dask_runs.csv'
FINAL_REPORT_FILE_PATH = 'results/cond_results.csv'
EXPECTED_SIZES = [3 * 3 * 10**x for x in range(1, 5 + 2)]


@dataclass(frozen=True)
class PersistedRunResult:
    strategy_name: str
    language: str
    engine: CalcEngine
    strategy_w_language_name: str
    interface: str
    dataSize: int
    elapsedTime: float
    recordCount: int


def derive_run_log_file_path(
        engine: CalcEngine,
) -> str:
    match engine:
        case  CalcEngine.PYSPARK:
            run_log = T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH
        case CalcEngine.DASK:
            run_log = T_PYTHON_DASK_RUN_LOG_FILE_PATH
        case _:
            raise ValueError(f"Unknown engine: {engine}")
    return os.path.join(
        root_folder_abs_path(),
        run_log)


def regressor_from_run_result(
        result: PersistedRunResult
) -> int:
    return result.dataSize
