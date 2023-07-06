import datetime
from typing import TextIO

from SixFieldTestData import RunResult
from .CondDirectory import PythonTestMethod


def write_header(file: TextIO):
    print(
        ' strategy,interface,dataSize,elapsedTime,recordCount,finishedAt,',
        file=file)
    file.flush()


def write_run_result(
    cond_method: PythonTestMethod,
        result: RunResult, file: TextIO):
    print("%s,%s,%d,%f,%d,%s" % (
        cond_method.strategy_name, cond_method.interface,
        result.dataSize, result.elapsedTime, result.recordCount,
        datetime.datetime.now().isoformat(),
    ), file=file)
    file.flush()
