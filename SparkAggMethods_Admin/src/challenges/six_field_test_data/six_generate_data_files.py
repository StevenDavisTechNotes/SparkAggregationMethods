import logging
import os
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet
from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import DATA_SIZES_LIST_BI_LEVEL
from spark_agg_methods_common_python.challenges.conditional.conditional_test_data_types import (
    DATA_SIZES_LIST_CONDITIONAL,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_domain_logic.merging_samples import (
    calculate_solutions_progressively,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SIX_TEST_SOURCE_DATA_PYARROW_SCHEMA, TARGET_PARQUET_BATCH_SIZE, SixTestDataSetDescription,
    six_derive_expected_answer_data_file_paths_csv, six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import DATA_SIZES_LIST_VANILLA
from spark_agg_methods_common_python.perf_test_common import Challenge

# cSpell: ignore arange, aggfunc

logger = logging.getLogger(__name__)


def _remove_data_files_for_size(
        data_description: SixTestDataSetDescription,
) -> None:
    source_file_paths = six_derive_source_test_data_file_path(data_description)
    if os.path.exists(source_file_paths.source_directory_path):
        shutil.rmtree(source_file_paths.source_directory_path)


def _generate_source_data_file_for_size(
        data_description: SixTestDataSetDescription,
) -> None:
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    repetition = data_description.points_per_index
    final_source_file_paths = six_derive_source_test_data_file_path(
        data_description=data_description,
        temp_file=False,
    )
    temp_source_file_paths = six_derive_source_test_data_file_path(
        data_description=data_description,
        temp_file=True,
    )
    if (os.path.exists(final_source_file_paths.source_file_path_parquet_modern)):
        return
    for path in final_source_file_paths.file_paths + temp_source_file_paths.file_paths:
        if os.path.exists(path):
            os.remove(path)
    num_data_points = num_grp_1 * num_grp_2 * repetition
    Path(final_source_file_paths.source_directory_path).mkdir(parents=True, exist_ok=True)
    parquet_file_modern = pyarrow.parquet.ParquetWriter(
        where=temp_source_file_paths.source_file_path_parquet_modern,
        schema=SIX_TEST_SOURCE_DATA_PYARROW_SCHEMA,
        compression="ZSTD",
    )
    for i_batch_start in range(0, num_data_points, TARGET_PARQUET_BATCH_SIZE):
        i_batch_end = min(i_batch_start + TARGET_PARQUET_BATCH_SIZE, num_data_points)
        batch_size = i_batch_end - i_batch_start
        batch_range = range(i_batch_start, i_batch_end)
        df = pd.DataFrame(
            data={
                "id": np.arange(start=i_batch_start, stop=i_batch_end, dtype=np.int32),
                "grp": np.array([i // num_grp_2 % num_grp_1 for i in batch_range], dtype=np.int32),
                "subgrp": np.array([i % num_grp_2 for i in batch_range], dtype=np.int32),
            },
            dtype=np.int32,
        )
        df['A'] = np.random.randint(1, repetition + 1, batch_size, dtype=np.int32)
        df['B'] = np.random.randint(1, repetition + 1, batch_size, dtype=np.int32)
        df['C'] = np.random.uniform(1, 10, batch_size)
        df['D'] = np.random.uniform(1, 10, batch_size)
        df['E'] = np.random.normal(0, 10, batch_size)
        df['F'] = np.random.normal(1, 10, batch_size)
        if i_batch_start == 0 and num_data_points < 1000:
            with open(temp_source_file_paths.source_file_path_csv, "wb") as fh:
                df.to_csv(fh, index=False)
            os.rename(
                temp_source_file_paths.source_file_path_csv,
                final_source_file_paths.source_file_path_csv)
        as_pyarrow_table = pyarrow.table(df)
        parquet_file_modern.write_table(as_pyarrow_table)
    parquet_file_modern.close()
    os.rename(
        temp_source_file_paths.source_file_path_parquet_modern,
        final_source_file_paths.source_file_path_parquet_modern)


def _generate_answer_file(data_description: SixTestDataSetDescription) -> None:
    answer_file_paths = six_derive_expected_answer_data_file_paths_csv(data_description)
    if all(os.path.exists(f) for f in answer_file_paths.values()):
        return
    for file_name in answer_file_paths.values():
        if os.path.exists(file_name):
            os.remove(file_name)
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    repetition = data_description.points_per_index
    logger.info(f"Generating 6 answer data for {num_grp_1}, {num_grp_2}, {repetition} ")
    answer = calculate_solutions_progressively(
        data_description,
        challenges=[Challenge.BI_LEVEL, Challenge.CONDITIONAL, Challenge.VANILLA],
    )
    answer[Challenge.BI_LEVEL].to_csv(answer_file_paths[Challenge.BI_LEVEL], index=False)
    answer[Challenge.CONDITIONAL].to_csv(answer_file_paths[Challenge.CONDITIONAL], index=False)
    answer[Challenge.VANILLA].to_csv(answer_file_paths[Challenge.VANILLA], index=False)


def six_generate_data_files(
        *,
        make_new_files: bool,
) -> None:
    data_descriptions = (
        DATA_SIZES_LIST_BI_LEVEL
        + DATA_SIZES_LIST_CONDITIONAL
        + DATA_SIZES_LIST_VANILLA
    )
    if make_new_files:
        for data_description in data_descriptions:
            _remove_data_files_for_size(data_description)
    for data_description in data_descriptions:
        _generate_source_data_file_for_size(data_description)
        _generate_answer_file(data_description)
