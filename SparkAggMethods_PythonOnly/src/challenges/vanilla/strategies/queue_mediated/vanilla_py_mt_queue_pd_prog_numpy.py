# usage: python -m src.challenges.vanilla.strategies.queue_mediated.vanilla_py_mt_queue_pd_prog_numpy

import logging
import queue

import pyarrow.parquet
from spark_agg_methods_common_python.challenges.six_field_test_data.six_domain_logic.merging_samples import (
    ProgressiveSixTestStage1BatchAccumulator, calculate_solutions_from_summary,
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
from src.utils.thread_backed_queues.linear_execution_graph import execute_in_three_stages

CHALLENGE = Challenge.VANILLA

logger = logging.getLogger(__name__)


def read_linearly_from_single_parquet_file(
        data_size: SixTestDataSetDescription,
        queue: queue.Queue[pyarrow.Table],
) -> None:
    source_file_names = six_derive_source_test_data_file_path(
        data_description=data_size,
    )
    parquet_file = pyarrow.parquet.ParquetFile(source_file_names.source_file_path_parquet_modern)
    for batch in parquet_file.iter_batches(batch_size=TARGET_PARQUET_BATCH_SIZE):
        if queue.is_shutdown:
            break
        queue.put(batch)


# def calculate_subtotals(
#         df_chunk: pd.DataFrame,
# ) -> None:
#     for np_key, df_group in df_chunk.groupby(by=['grp', 'subgrp']):
#         key = int(np_key[0]), int(np_key[1])
#         df_group_c = df_group.loc[:, 'C']
#         df_group_d = df_group.loc[:, 'D']
#         df_group_e = df_group.loc[:, 'E']
#         uncond_count_subtotal[key] += df_group.shape[0]
#         mean_c_subtotal[key].update(df_group_c)
#         max_d_subtotal[key].update(df_group_d)
#         uncond_var_e_subtotal[key].update(df_group_e)
#         if df_group.shape[0] > 1:
#             cond_var_e_subtotal[key].update(df_group_e)

def nop(*args, **kwargs):
    pass


def vanilla_py_mt_queue_pd_prog_numpy(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonOnly,
) -> TChallengePythonOnlyAnswer:
    num_threads = 1
    stage1_batch_accumulators = [
        ProgressiveSixTestStage1BatchAccumulator(
            include_conditional=False,
            include_unconditional=True,
        ) for _ in range(num_threads)
    ]

    def source_action(queue: queue.Queue[pyarrow.Table]):
        read_linearly_from_single_parquet_file(
            data_size=data_set.data_description,
            queue=queue,
        )

    success = execute_in_three_stages(
        actions_0=(source_action,),
        actions_1=(lambda table: table.to_pandas(),),
        actions_2=tuple(acc.update for acc in stage1_batch_accumulators),
        actions_3=(nop,),
        queue_0=queue.Queue(maxsize=3),
        queue_1=queue.Queue(maxsize=1),
        queue_2=queue.Queue(maxsize=1),
        report_error=lambda msg: logger.error(msg),
    )
    assert success
    num_data_points_visited, df_summary = stage1_batch_accumulators[0].summary()
    solutions = calculate_solutions_from_summary(
        data_size=data_set.data_description,
        challenges=[CHALLENGE],
        num_data_points_visited=num_data_points_visited,
        df_summary=df_summary,
    )
    return solutions[CHALLENGE]


if __name__ == "__main__":
    setup_logging()
    data_size = DATA_SIZES_LIST_VANILLA[0]
    source_file_names = six_derive_source_test_data_file_path(
        data_description=data_size,
    ).source_file_path_parquet_modern
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
