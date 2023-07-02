import datetime
from typing import TextIO

from .SectionTypeDefs import RunResult, PythonTestMethod

LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting"
REMOTE_TEST_DATA_LOCATION = "wasb:///sparkperftesting"
PYTHON_RESULT_FILE_PATH = 'Results/section_runs.csv'
FINAL_REPORT_FILE_PATH = '../Results/python/section_results_20230618.csv'

NumExecutors = 7
MaximumProcessableSegment = pow(10, 5)

def write_header(file: TextIO):
    print(
        ' strategy,interface,dataSize,relCard,elapsedTime,recordCount,finishedAt,',
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
