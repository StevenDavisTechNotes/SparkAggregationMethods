from typing import Iterable, TextIO

from dataclasses import dataclass
import datetime

from SixFieldCommon.SixFieldTestData import MAX_DATA_POINTS_PER_PARTITION, DataSet
from Utils.Utils import int_divide_round_up

from .BiLevelDirectory import PythonTestMethod

RESULT_FILE_PATH = 'Results/bi_level_runs.csv'
FINAL_REPORT_FILE_PATH = '../Results/python/bi_level_results_20230618.csv'
EXPECTED_SIZES = [1, 10, 100, 1000]


@dataclass(frozen=True)
class RunResult:
    dataSize: int
    relCard: int
    elapsedTime: float
    recordCount: int


@dataclass(frozen=True)
class PersistedRunResult:
    strategy_name: str
    interface: str
    dataSize: int
    relCard: int
    elapsedTime: float
    recordCount: int


def regressor_from_run_result(result: PersistedRunResult) -> int:
    return result.relCard


def infeasible(strategy_name: str, data_set: DataSet) -> bool:
    match strategy_name:
        case 'bi_rdd_grpmap':
            return (
                data_set.NumDataPoints
                > MAX_DATA_POINTS_PER_PARTITION
                * data_set.NumGroups)
        case _:
            return False


def write_header(file: TextIO):
    print(' strategy,interface,dataSize,relCard,elapsedTime,recordCount,finishedAt,', file=file)
    file.flush()


def write_run_result(
        test_method: PythonTestMethod,
        result: RunResult,
        file: TextIO
):
    print("%s,%s,%d,%d,%f,%d,%s," % (
        test_method.strategy_name, test_method.interface,
        result.dataSize, result.relCard, result.elapsedTime, result.recordCount,
        datetime.datetime.now().isoformat(),
    ), file=file)
    file.flush()


def read_result_file() -> Iterable[PersistedRunResult]:
    with open(RESULT_FILE_PATH, 'r', encoding='utf-8-sig') as f:
        for textline in f:
            textline = textline.rstrip()
            if textline.startswith('#'):
                print("Excluding line: " + textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: " + textline)
                continue
            fields = textline.split(',')
            if len(fields) < 6:
                fields.append('3')
            strategy_name, interface, dataSize, relCard, elapsedTime, recordCount, *rest = fields
            if recordCount != '3':
                print("Excluding line: " + textline)
                continue
            yield PersistedRunResult(
                strategy_name=strategy_name,
                interface=interface,
                dataSize=int(dataSize),
                relCard=int(relCard),
                elapsedTime=float(elapsedTime),
                recordCount=int(recordCount))
