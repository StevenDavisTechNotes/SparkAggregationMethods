import logging
import queue
import threading
from collections import defaultdict

import pandas as pd
import pyarrow.parquet
from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import DATA_SIZES_LIST_BI_LEVEL
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    TARGET_PARQUET_BATCH_SIZE, SixTestDataSetDescription, six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.utils.progressive_statistics.progressive_max import ProgressiveMax
from spark_agg_methods_common_python.utils.progressive_statistics.progressive_mean import ProgressiveMean
from spark_agg_methods_common_python.utils.progressive_statistics.progressive_variance import ProgressiveVariance


class BatchAccumulator():
    include_conditional: bool
    include_unconditional: bool
    uncond_count_subtotal: dict[tuple[int, int], int]
    mean_c_subtotal: dict[tuple[int, int], ProgressiveMean]
    max_d_subtotal: dict[tuple[int, int], ProgressiveMax]
    uncond_var_e_subtotal: dict[tuple[int, int], ProgressiveVariance]
    cond_var_e_subtotal: dict[tuple[int, int], ProgressiveVariance]

    def __init__(
            self,
            include_conditional: bool,
            include_unconditional: bool,
    ):
        self.include_conditional = include_conditional
        self.include_unconditional = include_unconditional
        self.uncond_count_subtotal = defaultdict(lambda: 0)
        self.mean_c_subtotal = defaultdict(lambda: ProgressiveMean())
        self.max_d_subtotal = defaultdict(lambda: ProgressiveMax())
        self.uncond_var_e_subtotal = defaultdict(lambda: ProgressiveVariance(ddof=0))
        self.cond_var_e_subtotal = defaultdict(lambda: ProgressiveVariance(ddof=0))

    def update(self, df_group: pd.DataFrame):
        for np_key, df_group in df_group.groupby(by=['grp', 'subgrp']):
            key = int(np_key[0]), int(np_key[1])  # pyright: ignore[reportIndexIssue]
            df_group_c = df_group.loc[:, 'C']
            df_group_d = df_group.loc[:, 'D']
            self.uncond_count_subtotal[key] += len(df_group)
            self.mean_c_subtotal[key].update(df_group_c)
            self.max_d_subtotal[key].update(df_group_d)
            if self.include_conditional:
                cond_df_group_e = df_group.loc[df_group["E"] < 0, 'E']
                self.cond_var_e_subtotal[key].update(cond_df_group_e.to_numpy())
            if self.include_unconditional:
                uncond_df_group_e = df_group.loc[:, 'E']
                self.uncond_var_e_subtotal[key].update(uncond_df_group_e.to_numpy())


class SubtotalAccumulator():
    include_conditional: bool
    include_unconditional: bool
    uncond_count_subtotal: dict[tuple[int, int], int]
    mean_c_subtotal: dict[tuple[int, int], ProgressiveMean]
    max_d_subtotal: dict[tuple[int, int], ProgressiveMax]
    uncond_var_e_subtotal: dict[tuple[int, int], ProgressiveVariance]
    cond_var_e_subtotal: dict[tuple[int, int], ProgressiveVariance]

    def __init__(
            self,
            include_conditional: bool,
            include_unconditional: bool,
    ):
        self.include_conditional = include_conditional
        self.include_unconditional = include_unconditional
        self.uncond_count_subtotal = defaultdict(lambda: 0)
        self.mean_c_subtotal = defaultdict(lambda: ProgressiveMean())
        self.max_d_subtotal = defaultdict(lambda: ProgressiveMax())
        self.uncond_var_e_subtotal = defaultdict(lambda: ProgressiveVariance(ddof=0))
        self.cond_var_e_subtotal = defaultdict(lambda: ProgressiveVariance(ddof=0))

    def update(self, sample: BatchAccumulator):
        left = self
        right = sample
        for keys in right.mean_c_subtotal.keys():
            left.uncond_count_subtotal[keys] += right.uncond_count_subtotal[keys]
            left.mean_c_subtotal[keys].merge_subtotals(right.mean_c_subtotal[keys])
            left.max_d_subtotal[keys].merge_subtotals(right.max_d_subtotal[keys])
            if self.include_conditional:
                left.cond_var_e_subtotal[keys].merge_subtotals(right.cond_var_e_subtotal[keys])
            if self.include_unconditional:
                left.uncond_var_e_subtotal[keys].merge_subtotals(right.uncond_var_e_subtotal[keys])

    def finalize(self) -> tuple[int, pd.DataFrame]:
        num_data_points_visited = sum(self.uncond_count_subtotal.values())
        df_summary = (
            pd.DataFrame.from_records([
                {
                    "grp": key[0],
                    "subgrp": key[1],
                    "mean_of_C": self.mean_c_subtotal[key].mean,
                    "max_of_D": self.max_d_subtotal[key].max,
                } | ({
                    "cond_var_of_E": self.cond_var_e_subtotal[key].variance,
                } if self.include_conditional else {}
                ) | ({
                    "uncond_var_of_E": self.uncond_var_e_subtotal[key].variance,
                } if self.include_unconditional else {})
                for key in self.uncond_count_subtotal
            ]
            )
            .sort_values(by=["grp", "subgrp"])
        )
        return num_data_points_visited, df_summary


class ProducerConsumerBatchStrategyNetwork:
    include_conditional: bool
    include_unconditional: bool
    data_size: SixTestDataSetDescription
    queue_stage_1: queue.Queue[pd.DataFrame]
    threads_stage_1: threading.Thread | None = None
    queue_stage_2: queue.Queue[BatchAccumulator]
    threads_stage_2: list[threading.Thread] | None = None
    queue_stage_3: queue.Queue[SubtotalAccumulator]
    threads_stage_3: threading.Thread | None = None
    final_accumulator: SubtotalAccumulator
    running: bool

    def __init__(
            self,
            *,
            data_size: SixTestDataSetDescription,
            include_conditional: bool,
            include_unconditional: bool,
            num_accumulator_threads: int,
    ):
        self.data_size = data_size
        self.include_conditional = include_conditional
        self.include_unconditional = include_unconditional
        self.queue_stage_1 = queue.Queue()
        self.queue_stage_2 = queue.Queue()
        self.queue_stage_3 = queue.Queue()
        self.threads_stage_1 = (
            threading.Thread(target=self.stage_1_action, args=())
            if num_accumulator_threads > 0 else None
        )
        self.threads_stage_2 = (
            [
                threading.Thread(target=self.stage_2_action, args=())
                for _ in range(num_accumulator_threads)
            ]
            if num_accumulator_threads > 0 else None
        )
        self.threads_stage_3 = (
            threading.Thread(target=self.stage_3_action, args=())
            if num_accumulator_threads > 0 else None
        )
        self.final_accumulator = SubtotalAccumulator(
            include_conditional=self.include_conditional,
            include_unconditional=self.include_unconditional,
        )

    def stage_1_action(self):
        source_file_names = six_derive_source_test_data_file_path(
            data_description=self.data_size,
        )
        parquet_file = pyarrow.parquet.ParquetFile(source_file_names.source_file_path_parquet_modern)
        for table in parquet_file.iter_batches(batch_size=TARGET_PARQUET_BATCH_SIZE):
            if not self.running:
                return
            df = table.to_pandas()
            self.queue_stage_1.put(df)

    def stage_2_action(self):
        accumulator = BatchAccumulator(
            include_conditional=self.include_conditional,
            include_unconditional=self.include_unconditional,
        )
        while True:
            try:
                df = self.queue_stage_1.get()
                if not self.running:
                    return
                accumulator.update(df)
                self.queue_stage_1.task_done()
            except queue.ShutDown:
                break
        self.queue_stage_2.put(accumulator)

    def stage_3_action(self):
        while True:
            try:
                subtotal = self.queue_stage_2.get()
                if not self.running:
                    return
                self.final_accumulator.update(subtotal)
                self.queue_stage_2.task_done()
            except queue.ShutDown:
                break

    def __enter__(self):
        self.running = True
        if self.threads_stage_1 is not None:
            self.threads_stage_1.start()
        if self.threads_stage_2 is not None:
            for thread in self.threads_stage_2:
                thread.start()
        if self.threads_stage_3 is not None:
            self.threads_stage_3.start()
        return self

    def wait_for_completion(self) -> tuple[int, pd.DataFrame]:
        if self.threads_stage_1 is None:
            self.stage_1_action()
        else:
            self.threads_stage_1.join()
        self.queue_stage_1.join()
        self.queue_stage_2.join()
        self.queue_stage_2.shutdown()
        if self.threads_stage_2 is None:
            self.stage_2_action()
        else:
            for thread in self.threads_stage_2:
                thread.join()
        self.queue_stage_3.join()
        self.queue_stage_3.shutdown()
        if self.threads_stage_3 is None:
            self.stage_3_action()
        else:
            self.threads_stage_3.join()
        return self.final_accumulator.finalize()

    def __exit__(self, exc_type, exc_value, traceback):
        self.running = False
        if self.threads_stage_1 is not None:
            if self.threads_stage_1.is_alive():
                self.threads_stage_1.join()
        if self.threads_stage_2 is not None:
            for thread in self.threads_stage_2:
                if thread.is_alive():
                    thread.join()
        if self.threads_stage_3 is not None:
            if self.threads_stage_3.is_alive():
                self.threads_stage_3.join()


if __name__ == "--main__":
    logging.basicConfig(level=logging.INFO)
    data_size = DATA_SIZES_LIST_BI_LEVEL[0]
    with ProducerConsumerBatchStrategyNetwork(
            data_size=data_size,
            include_conditional=False,
            include_unconditional=True,
            num_accumulator_threads=0,
    ) as network:
        num_data_points_visited, df_summary = network.wait_for_completion()
        print(num_data_points_visited)
        print(df_summary)
