from typing import TextIO

from dataclasses import dataclass
import datetime

from .BiLevelDirectory import PythonTestMethod

RESULT_FILE_PATH = 'Results/bi_level_runs.csv'
FINAL_REPORT_FILE_PATH = '../Results/python/bi_level_results_20230618.csv'


@dataclass(frozen=True)
class RunResult:
    dataSize: int
    relCard: int
    elapsedTime: float
    recordCount: int


def write_header(file: TextIO):
    print(' strategy,interface,dataSize,relCard,elapsedTime,recordCount,finishedAt,', file=file)
    file.flush()


def write_run_result(cond_method: PythonTestMethod, result: RunResult, file: TextIO):
    print("%s,%s,%d,%d,%f,%d,%s," % (
        cond_method.strategy_name, cond_method.interface,
        result.dataSize, result.relCard, result.elapsedTime, result.recordCount,
        datetime.datetime.now().isoformat(),
    ), file=file)
    file.flush()
