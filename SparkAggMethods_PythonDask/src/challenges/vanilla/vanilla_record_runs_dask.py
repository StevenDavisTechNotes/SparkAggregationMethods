import os

from spark_agg_methods_common_python.challenges.vanilla.vanilla_record_runs import VanillaPythonRunResultFileWriter
from spark_agg_methods_common_python.perf_test_common import CalcEngine

ENGINE = CalcEngine.DASK


class VanillaDaskRunResultFileWriter(VanillaPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/vanilla_dask_runs.csv')

    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=__class__.RUN_LOG_FILE_PATH,
        )
