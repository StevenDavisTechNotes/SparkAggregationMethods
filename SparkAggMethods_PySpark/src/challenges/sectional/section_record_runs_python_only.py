from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

from src.challenges.sectional.section_record_runs import (
    SectionPythonPersistedRunResultLog, SectionPythonRunResultFileWriter,
)

SECTION_PYTHON_ONLY_RUN_LOG_FILE_PATH = 'results/section_python_only_runs.csv'
LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYTHON_ONLY


class SectionPythonOnlyPersistedRunResultLog(SectionPythonPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=SECTION_PYTHON_ONLY_RUN_LOG_FILE_PATH,
        )


class SectionPythonOnlyRunResultFileWriter(SectionPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=SECTION_PYTHON_ONLY_RUN_LOG_FILE_PATH,
        )
