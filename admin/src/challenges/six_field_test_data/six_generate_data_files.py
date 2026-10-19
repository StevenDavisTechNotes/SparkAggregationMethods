import logging
import shutil

import numpy as np
import pandas as pd
import pyarrow.parquet
from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import (
    DATA_SIZES_LIST_BI_LEVEL_MAXIMAL, DATA_SIZES_LIST_BI_LEVEL_MINIMAL,
)
from spark_agg_methods_common_python.challenges.conditional.conditional_test_data_types import (
    DATA_SIZES_LIST_CONDITIONAL_MAXIMAL, DATA_SIZES_LIST_CONDITIONAL_MINIMAL,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_domain_logic.merging_samples import (
    calculate_solutions_progressively,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    TARGET_PARQUET_BATCH_SIZE, SixTestDataSetDescription, six_derive_expected_answer_data_file_paths_csv,
    six_derive_source_test_data_directory_path, six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    DATA_SIZES_LIST_VANILLA_MAXIMAL, DATA_SIZES_LIST_VANILLA_MINIMAL,
)
from spark_agg_methods_common_python.perf_test_common import CHALLENGES_SIX_DATA

# cSpell: ignore arange, aggfunc

logger = logging.getLogger(__name__)


SIX_TEST_SOURCE_DATA_PYARROW_SCHEMA = pyarrow.schema([
    pyarrow.field("id", pyarrow.int32()),
    pyarrow.field("grp", pyarrow.int32()),
    pyarrow.field("subgrp", pyarrow.int32()),
    pyarrow.field("A", pyarrow.int32()),
    pyarrow.field("B", pyarrow.int32()),
    pyarrow.field("C", pyarrow.float64()),
    pyarrow.field("D", pyarrow.float64()),
    pyarrow.field("E", pyarrow.float64()),
    pyarrow.field("F", pyarrow.float64()),
])


def _remove_data_files_for_size(
        data_description: SixTestDataSetDescription,
) -> None:
    source_file_paths = six_derive_source_test_data_file_path(data_description)
    if source_file_paths.source_directory_path.exists():
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
    if final_source_file_paths.source_file_path_parquet_single_file.exists():
        return
    for path in final_source_file_paths.file_paths + temp_source_file_paths.file_paths:
        path.unlink(missing_ok=True)
    num_data_points = num_grp_1 * num_grp_2 * repetition
    final_source_file_paths.source_directory_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Creating {final_source_file_paths.source_file_path_parquet_single_file}")
    parquet_file_modern = pyarrow.parquet.ParquetWriter(
        where=temp_source_file_paths.source_file_path_parquet_single_file,
        schema=SIX_TEST_SOURCE_DATA_PYARROW_SCHEMA,
        compression="ZSTD",
    )
    for i_batch_start in range(0, num_data_points, TARGET_PARQUET_BATCH_SIZE):
        i_batch_end = min(i_batch_start + TARGET_PARQUET_BATCH_SIZE, num_data_points)
        batch_size = i_batch_end - i_batch_start
        batch_range = range(i_batch_start, i_batch_end)
        id_range = np.arange(
            start=i_batch_start, stop=i_batch_end, dtype=np.int32
        )  # pyright: ignore # ty: ignore[no-matching-overload]
        df = pd.DataFrame(
            data={
                "id": id_range,
                "grp": np.array([i // num_grp_2 % num_grp_1 for i in batch_range], dtype=np.int32),
                "subgrp": np.array([i % num_grp_2 for i in batch_range], dtype=np.int32),
            },
            dtype=np.int32,
        )
        df["A"] = np.random.randint(1, repetition + 1, batch_size, dtype=np.int32)
        df["B"] = np.random.randint(1, repetition + 1, batch_size, dtype=np.int32)
        df["C"] = np.random.uniform(1, 10, batch_size)
        df["D"] = np.random.uniform(1, 10, batch_size)
        df["E"] = np.random.normal(0, 10, batch_size)
        df["F"] = np.random.normal(1, 10, batch_size)
        if i_batch_start == 0 and num_data_points < 1000:
            logger.info(f"Creating {final_source_file_paths.source_file_path_csv}")
            with temp_source_file_paths.source_file_path_csv.open("wb") as fh:
                df.to_csv(fh, index=False)
            temp_source_file_paths.source_file_path_csv.rename(
                final_source_file_paths.source_file_path_csv)
        as_pyarrow_table = pyarrow.table(df)
        parquet_file_modern.write_table(as_pyarrow_table)
    parquet_file_modern.close()
    temp_source_file_paths.source_file_path_parquet_single_file.rename(
        final_source_file_paths.source_file_path_parquet_single_file)


def _generate_answer_file(data_description: SixTestDataSetDescription) -> None:
    answer_file_paths = six_derive_expected_answer_data_file_paths_csv(data_description)
    if all(f.exists() for f in answer_file_paths.values()):
        return
    for file_name in answer_file_paths.values():
        file_name.unlink(missing_ok=True)
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    repetition = data_description.points_per_index
    logger.info(f"Generating 6 answer data for {num_grp_1}, {num_grp_2}, {repetition} ")
    answer = calculate_solutions_progressively(
        data_size=data_description,
        challenges=CHALLENGES_SIX_DATA,
    )
    for challenge in CHALLENGES_SIX_DATA:
        logger.info(f"Creating {answer_file_paths[challenge]}")
        answer[challenge].to_csv(answer_file_paths[challenge], index=False)


def six_generate_data_files_remove() -> None:
    root_path = six_derive_source_test_data_directory_path()
    if root_path.exists():
        shutil.rmtree(root_path)


def six_generate_data_files(
        *,
        overwrite: bool = False,
        minimal: bool = False,
) -> None:
    if overwrite:
        six_generate_data_files_remove()
    data_descriptions = (
        DATA_SIZES_LIST_BI_LEVEL_MINIMAL
        + DATA_SIZES_LIST_CONDITIONAL_MINIMAL
        + DATA_SIZES_LIST_VANILLA_MINIMAL
        if minimal else
        DATA_SIZES_LIST_BI_LEVEL_MAXIMAL
        + DATA_SIZES_LIST_CONDITIONAL_MAXIMAL
        + DATA_SIZES_LIST_VANILLA_MAXIMAL
    )
    for data_description in data_descriptions:
        _generate_source_data_file_for_size(data_description)
        _generate_answer_file(data_description)
