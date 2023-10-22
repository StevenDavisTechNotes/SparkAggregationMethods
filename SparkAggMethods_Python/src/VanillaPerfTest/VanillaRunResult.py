from dataclasses import dataclass

from PerfTestCommon import CalcEngine
from SixFieldCommon.Dask_SixFieldTestData import DaskDataSet
from SixFieldCommon.PySpark_SixFieldTestData import PysparkDataSet
from SixFieldCommon.SixFieldTestData import MAX_DATA_POINTS_PER_PARTITION

T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'Results/vanilla_pyspark_runs.csv'
T_PYTHON_DASK_RUN_LOG_FILE_PATH = 'Results/vanilla_dask_runs.csv'
SCALA_RUN_LOG_FILE_PATH = '../Results/Scala/vanilla_runs_scala.csv'
FINAL_REPORT_FILE_PATH = 'Results/vanilla_results.csv'
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


def run_log_file_path(
        engine: CalcEngine,
) -> str:
    match engine:
        case  CalcEngine.PYSPARK:
            return T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH
        case CalcEngine.DASK:
            return T_PYTHON_DASK_RUN_LOG_FILE_PATH
        case _:
            raise ValueError(f"Unknown engine: {engine}")


def regressor_from_run_result(
        result: PersistedRunResult,
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
        case 'vanilla_rdd_grpmap':
            return (
                data_set.description.NumDataPoints
                > MAX_DATA_POINTS_PER_PARTITION
                * data_set.description.NumGroups * data_set.description.NumSubGroups)
        case _:
            return False
