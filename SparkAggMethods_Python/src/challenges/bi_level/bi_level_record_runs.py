import datetime
import os
from dataclasses import dataclass
from typing import Iterable, TextIO

from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data_using_dask import \
    ChallengeMethodPythonDaskRegistration
from six_field_test_data.six_generate_test_data_using_pyspark import \
    ChallengeMethodPythonPysparkRegistration
from t_utils.t_utils import root_folder_abs_path

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


def write_header(
        file: TextIO
) -> None:
    print(' strategy,interface,dataSize,relCard,elapsedTime,recordCount,finishedAt,engine,', file=file)
    file.flush()


def write_run_result(
        challenge_method_registration: ChallengeMethodPythonPysparkRegistration | ChallengeMethodPythonDaskRegistration,
        result: RunResult,
        file: TextIO
) -> None:
    match challenge_method_registration:
        case ChallengeMethodPythonPysparkRegistration():
            engine = CalcEngine.PYSPARK.value
        case ChallengeMethodPythonDaskRegistration():
            engine = CalcEngine.DASK.value
        case _:
            raise ValueError(f"Unknown challenge_method_registration: {challenge_method_registration}")
    print("%s,%s,%d,%d,%f,%d,%s,%s," % (
        challenge_method_registration.strategy_name, challenge_method_registration.interface,
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
            for line in f:
                line = line.rstrip()
                if line.startswith('#'):
                    print("Excluding line: " + line)
                    continue
                if line.find(',') < 0:
                    print("Excluding line: " + line)
                    continue
                fields = line.split(',')
                if len(fields) < 6:
                    fields.append('3')
                strategy_name, interface, dataSize, relCard, elapsedTime, recordCount, *rest = fields
                if recordCount != '3':
                    print("Excluding line: " + line)
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
