from typing import TextIO
from dataclasses import dataclass
import datetime

from VanillaPerfTest.VanillaDirectory import PythonTestMethod

PYTHON_RESULT_FILE_PATH = 'Results/vanilla_runs.csv'
SCALA_RESULT_FILE_PATH = '../Results/Scala/vanilla_runs_scala.csv'
FINAL_REPORT_FILE_PATH = '../Results/python/vanilla_results_20230618.csv'


@dataclass(frozen=True)
class RunResult:
    dataSize: int
    elapsedTime: float
    recordCount: int


def write_header(file: TextIO):
    print(' strategy,interface,dataSize,elapsedTime,recordCount,finishedAt,', file=file)
    file.flush()


def write_run_result(cond_method: PythonTestMethod,
                     result: RunResult, file: TextIO):
    print("%s,%s,%d,%f,%d,%s," % (
        cond_method.strategy_name, cond_method.interface,
        result.dataSize, result.elapsedTime, result.recordCount,
        datetime.datetime.now().isoformat()
    ), file=file)
    file.flush()
