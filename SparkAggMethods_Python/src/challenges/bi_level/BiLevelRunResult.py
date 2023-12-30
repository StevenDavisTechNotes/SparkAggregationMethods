import datetime
import os
from dataclasses import dataclass
from typing import Iterable, TextIO

from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data_using_dask import (
    DaskDataSet, DaskPythonTestMethod)
from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, PysparkPythonTestMethod)
from six_field_test_data.six_test_data_types import \
    MAX_DATA_POINTS_PER_SPARK_PARTITION
from utils.Utils import root_folder_abs_path

# RESULT_FILE_PATH = 'results/bi_level_runs.csv'
T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/bi_level_pyspark_runs.csv'
T_PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/bi_level_dask_runs.csv'
FINAL_REPORT_FILE_PATH = 'results/bilevel_results.csv'
EXPECTED_SIZES = [1, 10, 100, 1000]


@dataclass(frozen=True)
class RunResult:
    engine: CalcEngine
    dataSize: int
    relCard: int
    elapsedTime: float
    recordCount: int


@dataclass(frozen=True)
class PersistedRunResult:
    strategy_name: str
    language: str
    engine: CalcEngine
    interface: str
    dataSize: int
    relCard: int
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
        result: PersistedRunResult
) -> int:
    return result.relCard


def dask_infeasible(
        strategy_name: str,
        data_set: DaskDataSet,
) -> bool:
    match strategy_name:
        case _:
            return False


def pyspark_infeasible(
        strategy_name: str,
        data_set: PysparkDataSet
) -> bool:
    match strategy_name:
        case 'bi_rdd_grpmap':
            return (
                data_set.description.NumDataPoints
                > MAX_DATA_POINTS_PER_SPARK_PARTITION
                * data_set.description.NumGroups)
        case _:
            return False


def write_header(
        file: TextIO
) -> None:
    print(' strategy,interface,dataSize,relCard,elapsedTime,recordCount,finishedAt,engine,', file=file)
    file.flush()


def write_run_result(
        test_method: PysparkPythonTestMethod | DaskPythonTestMethod,
        result: RunResult,
        file: TextIO
) -> None:
    match test_method:
        case PysparkPythonTestMethod():
            engine = CalcEngine.PYSPARK.value
        case DaskPythonTestMethod():
            engine = CalcEngine.DASK.value
        case _:
            raise ValueError(f"Unknown test_method: {test_method}")
    print("%s,%s,%d,%d,%f,%d,%s,%s," % (
        test_method.strategy_name, test_method.interface,
        result.dataSize, result.relCard, result.elapsedTime, result.recordCount,
        engine,
        datetime.datetime.now().isoformat(),
    ), file=file)
    file.flush()


def read_result_file() -> Iterable[PersistedRunResult]:
    for engine in [CalcEngine.PYSPARK, CalcEngine.DASK]:
        file_path = derive_run_log_file_path(engine)
        if os.path.exists(file_path) is False:
            return
        with open(file_path, 'r', encoding='utf-8-sig') as f:
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
                    language='python',
                    engine=engine,
                    interface=interface,
                    dataSize=int(dataSize),
                    relCard=int(relCard),
                    elapsedTime=float(elapsedTime),
                    recordCount=int(recordCount))
