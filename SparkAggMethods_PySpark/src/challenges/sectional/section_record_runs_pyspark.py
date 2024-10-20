from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

from src.challenges.sectional.section_record_runs import (
    SectionPythonPersistedRunResultLog, SectionPythonRunResultFileWriter,
)

SECTION_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/section_pyspark_runs.csv'
LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK
MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT: int = 5
MAXIMUM_PROCESSABLE_SEGMENT: int = 10**MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT


class SectionPysparkPersistedRunResultLog(SectionPythonPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=SECTION_PYTHON_PYSPARK_RUN_LOG_FILE_PATH,
        )


class SectionPysparkRunResultFileWriter(SectionPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=SECTION_PYTHON_PYSPARK_RUN_LOG_FILE_PATH,
        )
