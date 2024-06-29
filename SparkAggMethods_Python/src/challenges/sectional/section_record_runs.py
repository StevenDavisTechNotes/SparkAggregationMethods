import datetime
import os
from dataclasses import dataclass
from typing import TextIO

from challenges.sectional.section_test_data_types import (
    ChallengeMethodPysparkRegistration, DataSetDescription, RunResult)
from perf_test_common import CalcEngine
from t_utils.t_utils import root_folder_abs_path

T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH: str = 'results/section_pyspark_runs.csv'
T_PYTHON_DASK_RUN_LOG_FILE_PATH: str = 'results/section_dask_runs.csv'
FINAL_REPORT_FILE_PATH: str = 'results/section_results.csv'
MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT: int = 5
MAXIMUM_PROCESSABLE_SEGMENT: int = 10**MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT


@dataclass(frozen=True)
class PersistedRunResult:
    success: bool
    data: DataSetDescription
    elapsed_time: float
    record_count: int


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
        result: PersistedRunResult,
) -> int:
    return result.data.num_students


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
        data.description.num_students,
        data.description.num_rows,
        data.data.section_maximum,
        result.elapsed_time,
        result.record_count,
        datetime.datetime.now().isoformat()),
        file=file)
    file.flush()
