from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

from src.challenges.bi_level.bi_level_record_runs import BiLevelPersistedRunResultLog, BiLevelPythonRunResultFileWriter

BI_LEVEL_PYTHON_ONLY_RUN_LOG_FILE_PATH = 'results/bi_level_python_only_runs.csv'
LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYTHON_ONLY


class BiLevelPythonOnlyPersistedRunResultLog(BiLevelPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            language=LANGUAGE,
            rel_log_file_path=BI_LEVEL_PYTHON_ONLY_RUN_LOG_FILE_PATH,
        )


class BiLevelPythonOnlyRunResultFileWriter(BiLevelPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=BI_LEVEL_PYTHON_ONLY_RUN_LOG_FILE_PATH,
        )
