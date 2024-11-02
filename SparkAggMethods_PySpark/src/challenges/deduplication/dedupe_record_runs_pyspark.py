import os

from spark_agg_methods_common_python.challenges.deduplication.dedupe_record_runs import DedupePythonRunResultFileWriter
from spark_agg_methods_common_python.perf_test_common import CalcEngine

ENGINE = CalcEngine.PYSPARK


class DedupePysparkRunResultFileWriter(DedupePythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/dedupe_pyspark_runs.csv')

    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=__class__.RUN_LOG_FILE_PATH,
        )
