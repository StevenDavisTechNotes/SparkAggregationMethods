import datetime
from typing import TextIO

from PerfTestCommon import CalcEngine
from SixFieldCommon.Dask_SixFieldTestData import DaskPythonTestMethod
from SixFieldCommon.PySpark_SixFieldTestData import PysparkPythonTestMethod
from SixFieldCommon.SixFieldTestData import RunResult


def write_header(
        file: TextIO,
) -> None:
    print(' strategy,interface,dataSize,elapsedTime,recordCount,finishedAt,engine,', file=file)
    file.flush()


def write_run_result(
        test_method: PysparkPythonTestMethod | DaskPythonTestMethod,
        result: RunResult,
        file: TextIO,
) -> None:
    match test_method:
        case PysparkPythonTestMethod():
            engine = CalcEngine.PYSPARK
        case DaskPythonTestMethod():
            engine = CalcEngine.DASK
        case _:  # pylance: ignore[reportUnnecessaryComparison]
            raise ValueError(f"Unknown test_method: {test_method}")
    assert engine == result.engine
    print("%s,%s,%d,%f,%d,%s,%s," % (
        test_method.strategy_name, test_method.interface,
        result.dataSize, result.elapsedTime, result.recordCount,
        engine.value,
        datetime.datetime.now().isoformat()
    ), file=file)
    file.flush()
