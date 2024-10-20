from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

from spark_agg_methods_common_python.challenges.conditional.conditional_record_runs import (
    ConditionalPersistedRunResultLog, ConditionalPythonRunResultFileWriter,
)

CONDITIONAL_PYTHON_ONLY_RUN_LOG_FILE_PATH = 'results/conditional_python_only_runs.csv'
LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYTHON_ONLY


class ConditionalPythonOnlyPersistedRunResultLog(ConditionalPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            language=LANGUAGE,
            rel_log_file_path=CONDITIONAL_PYTHON_ONLY_RUN_LOG_FILE_PATH,
        )


class ConditionalPythonOnlyRunResultFileWriter(ConditionalPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=CONDITIONAL_PYTHON_ONLY_RUN_LOG_FILE_PATH,
        )
