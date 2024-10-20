from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

from src.challenges.bi_level.bi_level_record_runs import BiLevelPersistedRunResultLog, BiLevelPythonRunResultFileWriter

BI_LEVEL_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/bi_level_pyspark_runs.csv'
LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK


class BiLevelPysparkPersistedRunResultLog(BiLevelPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            language=LANGUAGE,
            rel_log_file_path=BI_LEVEL_PYTHON_PYSPARK_RUN_LOG_FILE_PATH,
        )


class BiLevelPysparkRunResultFileWriter(BiLevelPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=BI_LEVEL_PYTHON_PYSPARK_RUN_LOG_FILE_PATH,
        )
