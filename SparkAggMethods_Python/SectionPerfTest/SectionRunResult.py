from dataclasses import dataclass
import datetime
from typing import Dict, TextIO

from .SectionTestData import LARGEST_EXPONENT
from .SectionTypeDefs import DataSetDescription, RunResult, PythonTestMethod

LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting"
REMOTE_TEST_DATA_LOCATION = "wasb:///sparkperftesting"
PYTHON_RESULT_FILE_PATH = 'Results/section_runs.csv'
FINAL_REPORT_FILE_PATH = '../Results/python/section_results_20230618.csv'

NumExecutors = 7
MaximumProcessableSegment = pow(10, 5)


@dataclass(frozen=True)
class PersistedRunResult:
    success: bool
    data: DataSetDescription
    elapsed_time: float
    record_count: int


def regressor_from_run_result(result: PersistedRunResult) -> int:
    return result.data.NumStudents


LARGEST_EXPONENT_BY_METHOD_NAME: Dict[str, int] = {
    'method_nospark_single_threaded': LARGEST_EXPONENT,
    'method_mappart_single_threaded': 5 - 1,  # unrealiable
    'method_join_groupby': 5 - 1,  # too slow
    'method_mappart_partials': 7 - 1,  # unrealiable
    'method_asymreduce_partials': 7 - 1,  # unrealiable
    'method_mappart_odd_even': 7 - 1,  # unrealiable
    'method_join_groupby': 7 - 1,  # times out
    'method_join_mappart': 7 - 1,  # times out
    'method_prep_mappart': 8 - 1,  # takes too long
    'method_prepcsv_groupby': 8 - 1,  # times out
    'method_prep_groupby': 8 - 1,  # times out
    'method_prepcsv_groupby': LARGEST_EXPONENT,
}


def infeasible(strategy: str, data_set: DataSetDescription) -> bool:
    dataNumStudents = data_set.dataSize // data_set.sectionMaximum
    largest_exponent_by_method_name = LARGEST_EXPONENT_BY_METHOD_NAME[strategy]
    return dataNumStudents >= pow(10, largest_exponent_by_method_name)


# EXPECTED_SIZES_BY_STRATEGY = {
#     strategy_name:
#     [10**x for x in range(1, max_exp)]
#     for strategy_name, max_exp in LARGEST_EXPONENT_BY_METHOD_NAME.items()
# }


# EXPECTED_SIZES = {
#     10**x
#     for max_exp in LARGEST_EXPONENT_BY_METHOD_NAME.values()
#     for x in range(0, max_exp+1)
# }


def write_header(file: TextIO):
    print(
        ' status,strategy,interface,NumStudents,dataSize,sectionMaximum,elapsedTime,recordCount,finishedAt,',
        file=file)
    file.flush()


def write_run_result(test_method: PythonTestMethod,
                     result: RunResult, file: TextIO):
    data = result.data
    print("%s,%s,%s,%d,%d,%d,%f,%d,%s," % (
        "success" if result.success else "failure",
        test_method.strategy_name,
        test_method.interface,
        data.NumStudents,
        data.dataSize,
        data.sectionMaximum,
        result.elapsed_time,
        result.record_count,
        datetime.datetime.now().isoformat()),
        file=file)
    file.flush()
