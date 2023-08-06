from dataclasses import dataclass
import datetime
from typing import TextIO

from SixFieldCommon.SixFieldTestData import MAX_DATA_POINTS_PER_PARTITION, DataSet, PythonTestMethod, RunResult


PYTHON_RESULT_FILE_PATH = 'Results/vanilla_runs.csv'
SCALA_RESULT_FILE_PATH = '../Results/Scala/vanilla_runs_scala.csv'
FINAL_REPORT_FILE_PATH = '../Results/python/vanilla_results_20230618.csv'
EXPECTED_SIZES = [3 * 3 * 10**x for x in range(0, 6 + 1)]


@dataclass(frozen=True)
class PersistedRunResult:
    strategy_name: str
    language: str
    strategy_w_language_name: str
    interface: str
    dataSize: int
    elapsedTime: float
    recordCount: int


def regressor_from_run_result(result: PersistedRunResult) -> int:
    return result.dataSize


def infeasible(strategy_name: str, data_set: DataSet) -> bool:
    match strategy_name:
        case 'vanilla_rdd_grpmap':
            return (
                data_set.description.NumDataPoints
                > MAX_DATA_POINTS_PER_PARTITION
                * data_set.description.NumGroups * data_set.description.NumSubGroups)
        case _:
            return False


def write_header(file: TextIO):
    print(
        ' strategy,interface,dataSize,elapsedTime,recordCount,finishedAt,',
        file=file)
    file.flush()


def write_run_result(
    cond_method: PythonTestMethod,
        result: RunResult, file: TextIO):
    print("%s,%s,%d,%f,%d,%s," % (
        cond_method.strategy_name, cond_method.interface,
        result.dataSize, result.elapsedTime, result.recordCount,
        datetime.datetime.now().isoformat()
    ), file=file)
    file.flush()
