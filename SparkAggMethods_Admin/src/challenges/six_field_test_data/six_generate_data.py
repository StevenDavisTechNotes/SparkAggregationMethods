import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet
from spark_agg_methods_common_python.challenges.six_field_test_data.six_domain_logic.merging_samples import (
    calculate_solutions_progressively,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SIX_TEST_SOURCE_DATA_PYARROW_SCHEMA, SixTestDataSetDescription, six_derive_expected_answer_data_file_path_csv,
    six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import Challenge

# cSpell: ignore arange, aggfunc

logger = logging.getLogger(__name__)


def generate_answer_file(data_size: SixTestDataSetDescription) -> None:
    answer_file_names = six_derive_expected_answer_data_file_path_csv(data_size)
    for file_name in answer_file_names.values():
        if os.path.exists(file_name):
            os.remove(file_name)
    num_grp_1 = data_size.num_grp_1
    num_grp_2 = data_size.num_grp_2
    repetition = data_size.points_per_index
    logger.info(f"Generating 6 data for {num_grp_1}, {num_grp_2}, {repetition} ")
    answer = calculate_solutions_progressively(
        data_size,
        challenges=[Challenge.BI_LEVEL, Challenge.CONDITIONAL, Challenge.VANILLA],
    )
    answer[Challenge.BI_LEVEL].to_csv(answer_file_names[Challenge.BI_LEVEL], index=False)
    answer[Challenge.CONDITIONAL].to_csv(answer_file_names[Challenge.CONDITIONAL], index=False)
    answer[Challenge.VANILLA].to_csv(answer_file_names[Challenge.VANILLA], index=False)


def generate_source_data_file(
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
    for path in final_source_file_paths.file_paths + temp_source_file_paths.file_paths:
        if os.path.exists(path):
            os.remove(path)
    num_data_points = num_grp_1 * num_grp_2 * repetition
    Path(final_source_file_paths.source_directory_path).mkdir(parents=True, exist_ok=True)
    parquet_file_original = pyarrow.parquet.ParquetWriter(
        temp_source_file_paths.source_file_path_parquet_original,
        SIX_TEST_SOURCE_DATA_PYARROW_SCHEMA,
        version="1.0",
    )
    parquet_file_modern = pyarrow.parquet.ParquetWriter(
        temp_source_file_paths.source_file_path_parquet_modern,
        SIX_TEST_SOURCE_DATA_PYARROW_SCHEMA,
    )
    for i_batch_start in range(0, num_data_points, 10000):
        i_batch_end = min(i_batch_start + 10000, num_data_points)
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
        parquet_file_original.write_table(as_pyarrow_table)
        parquet_file_modern.write_table(as_pyarrow_table)
    parquet_file_original.close()
    os.rename(
        temp_source_file_paths.source_file_path_parquet_original,
        final_source_file_paths.source_file_path_parquet_original)
    parquet_file_modern.close()
    os.rename(
        temp_source_file_paths.source_file_path_parquet_modern,
        final_source_file_paths.source_file_path_parquet_modern)


def six_generate_data_file(
        *,
        data_description: SixTestDataSetDescription,
        make_new_files: bool,
) -> None:
    source_file_paths = six_derive_source_test_data_file_path(
        data_description=data_description,
    )
    if (
        make_new_files
        or not os.path.exists(source_file_paths.source_file_path_parquet_original)
        or not os.path.exists(source_file_paths.source_file_path_parquet_modern)
    ):
        generate_source_data_file(data_description)
    answer_file_names = six_derive_expected_answer_data_file_path_csv(data_description)
    if make_new_files or any(not os.path.exists(x) for x in answer_file_names.values()):
        generate_answer_file(data_description)
