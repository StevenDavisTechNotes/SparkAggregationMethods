import os
from dataclasses import dataclass

from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data_using_dask import DaskDataSet
from six_field_test_data.six_generate_test_data_using_pyspark import \
    PysparkDataSet
from six_field_test_data.six_test_data_types import \
    MAX_DATA_POINTS_PER_SPARK_PARTITION
from utils.utils import root_folder_abs_path

T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/vanilla_pyspark_runs.csv'
T_PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/vanilla_dask_runs.csv'
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
        result: PersistedRunResult,
) -> int:
    return result.dataSize


def dask_infeasible(
        strategy_name: str,
        data_set: DaskDataSet,
) -> bool:
    match strategy_name:
        case 'da_vanilla_pandas':
            return (
                data_set.description.NumDataPoints // data_set.description.NumGroups // data_set.description.NumSubGroups
                > 10**4
            )
        case _:
            return False


def pyspark_infeasible(
        strategy_name: str,
        data_set: PysparkDataSet,
) -> bool:
    match strategy_name:
        case 'vanilla_rdd_grpmap':
            return (
                data_set.description.NumDataPoints
                > MAX_DATA_POINTS_PER_SPARK_PARTITION
                * data_set.description.NumGroups * data_set.description.NumSubGroups)
        case _:
            return False
