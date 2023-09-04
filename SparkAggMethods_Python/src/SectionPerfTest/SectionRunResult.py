from dataclasses import dataclass
import datetime
from typing import Dict, TextIO

from SectionPerfTest.SectionTestData import LARGEST_EXPONENT
from SectionPerfTest.SectionTypeDefs import DataSetDescription, RunResult, PythonTestMethod


PYTHON_RESULT_FILE_PATH = 'Results/section_runs.csv'
FINAL_REPORT_FILE_PATH = '../Results/python/section_results_20230618.csv'
MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT = 5
MAXIMUM_PROCESSABLE_SEGMENT = 10**MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT


@dataclass(frozen=True)
class PersistedRunResult:
    success: bool
    data: DataSetDescription
    elapsed_time: float
    record_count: int


def regressor_from_run_result(
        result: PersistedRunResult,
) -> int:
    return result.data.num_students


LARGEST_EXPONENT_BY_METHOD_NAME: Dict[str, int] = {
    'section_nospark_single_threaded': LARGEST_EXPONENT,
    'section_mappart_single_threaded': MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT - 1,  # unrealiable
    'section_mappart_odd_even': 7 - 1,  # unrealiable
    'section_mappart_partials': 5 - 1,  # unrealiable in local mode
    'section_asymreduce_partials': 7 - 1,  # unrealiable
    'section_prep_mappart': 8 - 1,  # takes too long
    'section_prep_groupby': 8 - 1,  # times out
    'section_prepcsv_groupby': 8 - 1,  # times out
    'section_join_groupby': 5 - 1,  # too slow
    'section_join_mappart': 7 - 1,  # times out

}


def infeasible(
        strategy: str,
        data_set: DataSetDescription,
) -> bool:
    dataNumStudents = data_set.num_students
    largest_exponent_by_method_name = LARGEST_EXPONENT_BY_METHOD_NAME[strategy]
    if dataNumStudents > pow(10, largest_exponent_by_method_name):
        return True
    else:
        return False


def write_header(
        file: TextIO,
) -> None:
    print(
        ' status,strategy,interface,NumStudents,dataSize,sectionMaximum,elapsedTime,recordCount,finishedAt,',
        file=file)
    file.flush()


def write_run_result(
        test_method: PythonTestMethod,
        result: RunResult,
        file: TextIO,
) -> None:
    data = result.data
    print("%s,%s,%s,%d,%d,%d,%f,%d,%s," % (
        "success" if result.success else "failure",
        test_method.strategy_name,
        test_method.interface,
        data.description.num_students,
        data.description.num_rows,
        data.data.section_maximum,
        result.elapsed_time,
        result.record_count,
        datetime.datetime.now().isoformat()),
        file=file)
    file.flush()
