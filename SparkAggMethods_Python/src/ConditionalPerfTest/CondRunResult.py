import os
from dataclasses import dataclass

from PerfTestCommon import CalcEngine
from SixFieldCommon.Dask_SixFieldTestData import DaskDataSet
from SixFieldCommon.PySpark_SixFieldTestData import PysparkDataSet
from SixFieldCommon.SixFieldTestData import MAX_DATA_POINTS_PER_SPARK_PARTITION
from Utils.Utils import root_folder_abs_path

T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'Results/conditional_pyspark_runs.csv'
T_PYTHON_DASK_RUN_LOG_FILE_PATH = 'Results/conditional_dask_runs.csv'
FINAL_REPORT_FILE_PATH = 'Results/cond_results.csv'
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


def dask_infeasible(
        strategy_name: str,
        data_set: DaskDataSet,
) -> bool:
    match strategy_name:
        case _:
            return False


def pyspark_infeasible(
        strategy_name: str,
        data_set: PysparkDataSet,
) -> bool:
    match strategy_name:
        case 'cond_rdd_grpmap':
            return (
                data_set.description.NumDataPoints
                > MAX_DATA_POINTS_PER_SPARK_PARTITION
                * data_set.description.NumGroups)
        case _:
            return False
