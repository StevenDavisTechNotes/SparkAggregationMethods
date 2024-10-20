from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

from src.challenges.conditional.conditional_record_runs import (
    ConditionalPythonPersistedRunResultLog, ConditionalPythonRunResultFileWriter,
)

CONDITIONAL_PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/conditional_dask_runs.csv'

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.DASK


class BiLevelDaskPersistedRunResultLog(ConditionalPythonPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=CONDITIONAL_PYTHON_DASK_RUN_LOG_FILE_PATH,
        )


class BiLevelDaskRunResultFileWriter(ConditionalPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=CONDITIONAL_PYTHON_DASK_RUN_LOG_FILE_PATH,
        )
