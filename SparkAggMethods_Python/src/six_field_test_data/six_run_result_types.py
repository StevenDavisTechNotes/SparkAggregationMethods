import datetime
from typing import TextIO

from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data_using_dask import \
    DaskPythonTestMethod
from six_field_test_data.six_generate_test_data_using_pyspark import \
    PysparkPythonTestMethod
from six_field_test_data.six_test_data_types import RunResult


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
