from spark_agg_methods_common_python.challenges.deduplication.dedupe_record_runs import (
    DedupePersistedRunResultLog, DedupePythonRunResultFileWriter,
)
from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

DEDUPE_PYSPARK_RUN_LOG_FILE_PATH = 'results/dedupe_pyspark_runs.csv'
LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK


class DedupePysparkPersistedRunResultLog(DedupePersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            language=LANGUAGE,
            rel_log_file_path=DEDUPE_PYSPARK_RUN_LOG_FILE_PATH,
        )


class DedupePysparkRunResultFileWriter(DedupePythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=DEDUPE_PYSPARK_RUN_LOG_FILE_PATH,
        )
