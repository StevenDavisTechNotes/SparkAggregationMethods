import os

from spark_agg_methods_common_python.challenges.sectional.section_record_runs import SectionPythonRunResultFileWriter
from spark_agg_methods_common_python.perf_test_common import CalcEngine

ENGINE = CalcEngine.PYTHON_ONLY


class SectionPythonOnlyRunResultFileWriter(SectionPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/section_python_only_runs.csv')

    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=__class__.RUN_LOG_FILE_PATH,
        )
