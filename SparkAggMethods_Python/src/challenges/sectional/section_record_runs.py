import datetime
import os
from dataclasses import dataclass
from typing import TextIO

from src.challenges.sectional.section_test_data_types import (
    ChallengeMethodPysparkRegistration, DataSetDescription, RunResult)
from src.perf_test_common import CalcEngine
from src.utils.utils import root_folder_abs_path

T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH: str = 'results/section_pyspark_runs.csv'
T_PYTHON_DASK_RUN_LOG_FILE_PATH: str = 'results/section_dask_runs.csv'
FINAL_REPORT_FILE_PATH: str = 'results/section_results.csv'
MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT: int = 5
MAXIMUM_PROCESSABLE_SEGMENT: int = 10**MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT


@dataclass(frozen=True)
class PersistedRunResult:
    success: bool
    data_size: DataSetDescription
    elapsed_time: float
    record_count: int


def derive_run_log_file_path_for_recording(
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


def derive_run_log_file_path_for_reading(
        engine: CalcEngine,
) -> str | None:
    match engine:
        case CalcEngine.DASK | CalcEngine.PYSPARK:
            return derive_run_log_file_path_for_recording(engine)
        case CalcEngine.PYTHON_ONLY:
            return None
        case CalcEngine.SCALA_SPARK:
            return None
        case _:
            raise ValueError(f"Unknown engine: {engine}")


def regressor_from_run_result(
        result: PersistedRunResult,
) -> int:
    return result.data_size.num_students


def write_header(
        file: TextIO,
) -> None:
    print(
        ' status,strategy,interface,NumStudents,dataSize,sectionMaximum,elapsedTime,recordCount,finishedAt,',
        file=file)
    file.flush()


def write_run_result(
        challenge_method_registration: ChallengeMethodPysparkRegistration,
        result: RunResult,
        file: TextIO,
) -> None:
    data = result.data
    print("%s,%s,%s,%d,%d,%d,%f,%d,%s," % (
        "success" if result.success else "failure",
        challenge_method_registration.strategy_name,
        challenge_method_registration.interface,
        data.data_size.num_students,
        data.data_size.num_rows,
        data.data.section_maximum,
        result.elapsed_time,
        result.record_count,
        datetime.datetime.now().isoformat()),
        file=file)
    file.flush()
