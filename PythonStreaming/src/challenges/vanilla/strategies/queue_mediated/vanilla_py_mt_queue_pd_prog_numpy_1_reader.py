# usage: python -m src.challenges.vanilla.strategies.queue_mediated.vanilla_py_mt_queue_pd_prog_numpy_1_reader

import glob
import logging
import os
import queue
from typing import Generator

import pandas as pd
from simple_queued_pipelines.single_channel.sc_execution_graph import (
    execute_single_channel_linear_execution_graph_with_four_stages,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_domain_logic.merging_samples import (
    SixProgressiveBatchSampleStatistics, calculate_solutions_from_summary,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestDataSetDescription, SixTestExecutionParameters,
    six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    DATA_SIZES_LIST_VANILLA,
)
from spark_agg_methods_common_python.perf_test_common import (
    LOCAL_NUM_EXECUTORS, Challenge,
)
from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.six_field_test_data.six_test_data_for_py_stream import (
    SixDataSetDataPythonStreaming, SixDataSetPythonStreaming,
    TChallengePythonStreamAnswer,
)

CHALLENGE = Challenge.VANILLA

logger = logging.getLogger(__name__)


def read_separate_parquet_files_one_at_a_time(
        data_description: SixTestDataSetDescription,
) -> Generator[pd.DataFrame, None, None]:
    source_file_folder = six_derive_source_test_data_file_path(
        data_description=data_description,
    ).source_file_path_parquet_small_v1_files
    par_files = glob.glob(os.path.join(source_file_folder, "*.parquet"))
    for par_file_path in par_files:
        df = pd.read_parquet(path=par_file_path)
        yield df


def nop(*args, **kwargs):
    pass


def vanilla_py_mt_queue_pd_prog_numpy_1_reader(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonStreaming,
) -> TChallengePythonStreamAnswer:
    num_threads = 1
    stage1_batch_accumulators = [
        SixProgressiveBatchSampleStatistics(
            include_conditional=False,
            include_unconditional=True,
        ) for _ in range(num_threads)
    ]

    def source_action():
        return read_separate_parquet_files_one_at_a_time(
            data_description=data_set.data_description,
        )

    execute_single_channel_linear_execution_graph_with_four_stages(
        actions_0=(source_action,),
        actions_1=tuple(acc.update_with_population for acc in stage1_batch_accumulators),
        actions_2=(nop,),
        actions_3=(nop,),
        queue_0=queue.Queue(maxsize=3),
        queue_1=queue.Queue(maxsize=1),
        queue_2=queue.Queue(maxsize=1),
        report_error=lambda msg: logger.error(msg),
    )
    num_data_points_visited, df_summary = stage1_batch_accumulators[0].summary()
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
    df_result = vanilla_py_mt_queue_pd_prog_numpy_1_reader(
        exec_params=SixTestExecutionParameters(
            default_parallelism=0,
            num_executors=LOCAL_NUM_EXECUTORS,
        ),
        data_set=SixDataSetPythonStreaming(
            data_description=data_size,
            data=SixDataSetDataPythonStreaming(
                source_file_path_parquet=source_file_names,
            ),
        ),
    )
    print(df_result)
    print("Success!")


if __name__ == "__main__":
    setup_logging()
    test_main()
