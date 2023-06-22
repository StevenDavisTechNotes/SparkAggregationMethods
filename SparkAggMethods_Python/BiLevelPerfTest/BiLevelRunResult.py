from dataclasses import dataclass
from typing import TextIO

from .BiLevelDirectory import PythonTestMethod

RESULT_FILE_PATH = 'Results/bi_level_runs.csv'
FINAL_REPORT_FILE_PATH='../Results/python/bi_level_results_20230618.csv'

@dataclass(frozen=True)
class RunResult:
    dataSize: int
    relCard: int
    elapsedTime: float
    recordCount: int


def write_run_result(cond_method: PythonTestMethod, result: RunResult, file: TextIO):
    print("%s,%s,%d,%d,%f,%d" % (
        cond_method.name, cond_method.interface,
        result.dataSize, result.relCard, result.elapsedTime, result.recordCount), file=file)
    file.flush()
