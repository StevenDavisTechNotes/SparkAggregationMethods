from spark_agg_methods_common_python.challenges.conditional.conditional_record_runs import (
    ConditionalPythonPersistedRunResultLog, ConditionalPythonRunResultFileWriter,
)
from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

CONDITIONAL_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/conditional_pyspark_runs.csv'

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.DASK


class ConditionalPysparkPersistedRunResultLog(ConditionalPythonPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=CONDITIONAL_PYTHON_PYSPARK_RUN_LOG_FILE_PATH,
        )


class ConditionalPysparkRunResultFileWriter(ConditionalPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=CONDITIONAL_PYTHON_PYSPARK_RUN_LOG_FILE_PATH,
        )
