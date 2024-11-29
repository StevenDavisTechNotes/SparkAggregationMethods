# usage: python -m src.challenges.vanilla.strategies.queue_mediated.vanilla_py_mt_queue_pd_prog_1_reader_numpy

import glob
import logging
import os
import queue
from typing import Generator

import pandas as pd
import pyarrow.parquet
from simple_queued_pipelines.linear_execution_graph import execute_in_three_stages
from spark_agg_methods_common_python.challenges.six_field_test_data.six_domain_logic.merging_samples import (
    SixProgressiveBatchSampleStatistics, calculate_solutions_from_summary,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    TARGET_PARQUET_BATCH_SIZE, SixTestDataSetDescription, SixTestExecutionParameters,
    six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import DATA_SIZES_LIST_VANILLA
from spark_agg_methods_common_python.perf_test_common import LOCAL_NUM_EXECUTORS, Challenge
from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.six_field_test_data.six_test_data_for_py_only import (
    SixDataSetDataPythonOnly, SixDataSetPythonOnly, TChallengePythonOnlyAnswer,
)

CHALLENGE = Challenge.VANILLA

logger = logging.getLogger(__name__)


def read_linearly_from_single_parquet_file(
        data_size: SixTestDataSetDescription,
) -> Generator[pyarrow.Table]:
    source_file_names = six_derive_source_test_data_file_path(
        data_description=data_size,
    )
    parquet_file = pyarrow.parquet.ParquetFile(
        source_file_names.source_file_path_parquet_small_v1_files)
    return parquet_file.iter_batches(batch_size=TARGET_PARQUET_BATCH_SIZE)


def nop(*args, **kwargs):
    pass


def vanilla_py_mt_queue_pd_prog_numpy(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonOnly,
) -> TChallengePythonOnlyAnswer | None:
    num_file_reader_threads = 2
    num_compute_threads = 5
    stage1_batch_accumulators = [
        SixProgressiveBatchSampleStatistics(
            include_conditional=False,
            include_unconditional=True,
        ) for _ in range(num_compute_threads)
    ]

    def source_action():
        source_file_folder = six_derive_source_test_data_file_path(
            data_description=data_set.data_description,
        ).source_file_path_parquet_small_v1_files
        par_files = glob.glob(os.path.join(source_file_folder, "*.parquet"))
        for par_file_path in par_files:
            yield par_file_path

    def read_file(par_file_path: str) -> pd.DataFrame:
        parquet_file = pyarrow.parquet.ParquetFile(par_file_path)
        return parquet_file.read().to_pandas()

    max_num_data_frames_in_memory = (
        num_compute_threads  # number being processed
        + 1  # num waiting to keep memory down
    )
    execute_in_three_stages(
        actions_0=(source_action,),
        actions_1=(read_file,)*num_file_reader_threads,
        actions_2=tuple(acc.update_with_population for acc in stage1_batch_accumulators),
        actions_3=(nop,),
        queue_0=queue.Queue(maxsize=num_file_reader_threads),
        queue_1=queue.Queue(maxsize=max_num_data_frames_in_memory),
        queue_2=queue.Queue(maxsize=1),
        report_error=lambda msg: logger.error(msg),
    )
    final_summary = stage1_batch_accumulators[0]
    for summary in stage1_batch_accumulators[1:]:
        final_summary.merge_subtotals(summary)
    num_data_points_visited, df_summary = final_summary.summary()
    solutions = calculate_solutions_from_summary(
        data_size=data_set.data_description,
        challenges=[CHALLENGE],
        num_data_points_visited=num_data_points_visited,
        df_summary=df_summary,
    )
    return solutions[CHALLENGE]


def test_main():
    data_size = DATA_SIZES_LIST_VANILLA[0]
    source_file_names = six_derive_source_test_data_file_path(
        data_description=data_size,
    ).source_file_path_parquet_single_file
    df_result = vanilla_py_mt_queue_pd_prog_numpy(
        exec_params=SixTestExecutionParameters(
            default_parallelism=0,
            num_executors=LOCAL_NUM_EXECUTORS,
        ),
        data_set=SixDataSetPythonOnly(
            data_description=data_size,
            data=SixDataSetDataPythonOnly(
                source_file_path_parquet=source_file_names,
            ),
        ),
    )
    print(df_result)
    print("Success!")


if __name__ == "__main__":
    setup_logging()
    test_main()
