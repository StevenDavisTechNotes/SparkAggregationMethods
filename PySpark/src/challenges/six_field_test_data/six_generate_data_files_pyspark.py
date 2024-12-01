import logging
import os
import shutil
from pathlib import Path

import pyarrow.parquet
from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import DATA_SIZES_LIST_BI_LEVEL
from spark_agg_methods_common_python.challenges.conditional.conditional_test_data_types import (
    DATA_SIZES_LIST_CONDITIONAL,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestDataSetDescription, six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import DATA_SIZES_LIST_VANILLA

from src.utils.tidy_session_pyspark import TidySparkSession

# cSpell: ignore arange, aggfunc

logger = logging.getLogger(__name__)

BATCH_SIZE_FOR_SPARK: int = 10475   # Trying to keep the partition size < 1MB


def six_remove_source_data_file_for_size_pyspark(
        data_description: SixTestDataSetDescription,
) -> None:
    final_source_file_paths = six_derive_source_test_data_file_path(
        data_description=data_description,
        temp_file=False,
    )
    temp_source_file_paths = six_derive_source_test_data_file_path(
        data_description=data_description,
        temp_file=True,
    )
    for path in [
        final_source_file_paths.source_file_path_parquet_small_v1_files,
        temp_source_file_paths.source_file_path_parquet_small_v1_files
    ]:
        if os.path.exists(path):
            shutil.rmtree(path)


def six_generate_source_data_file_for_size_pyspark(
        data_description: SixTestDataSetDescription,
        spark_session: TidySparkSession,
) -> None:
    final_source_file_paths = six_derive_source_test_data_file_path(
        data_description=data_description,
        temp_file=False,
    )
    temp_source_file_paths = six_derive_source_test_data_file_path(
        data_description=data_description,
        temp_file=True,
    )
    read_file_path = final_source_file_paths.source_file_path_parquet_single_file
    write_folder_path = temp_source_file_paths.source_file_path_parquet_small_v1_files

    if (
        os.path.exists(write_folder_path)
        and sum([len(files) for r, d, files in os.walk(write_folder_path)]) > 0
    ):
        return
    if not os.path.exists(read_file_path):
        logger.error(f"Missing {read_file_path}")
        exit(1)
    six_remove_source_data_file_for_size_pyspark(data_description)
    Path(write_folder_path).parent.mkdir(parents=True, exist_ok=True)
    source_file = pyarrow.parquet.ParquetFile(read_file_path)
    for py_table in source_file.iter_batches(batch_size=BATCH_SIZE_FOR_SPARK):
        df = py_table.to_pandas()
        spark_df = spark_session.spark.createDataFrame(df)
        spark_df.write.parquet(
            write_folder_path,
            mode="append",
            compression="zstd",
        )
    source_file.close()
    os.rename(
        temp_source_file_paths.source_file_path_parquet_small_v1_files,
        final_source_file_paths.source_file_path_parquet_small_v1_files,
    )


def six_generate_data_files_pyspark(
        spark_session: TidySparkSession,
        make_new_files: bool,
) -> None:
    data_sizes = (
        DATA_SIZES_LIST_BI_LEVEL
        + DATA_SIZES_LIST_CONDITIONAL
        + DATA_SIZES_LIST_VANILLA
    )
    if make_new_files:
        for size in data_sizes:
            six_remove_source_data_file_for_size_pyspark(
                data_description=size,
            )
    for size in data_sizes:
        six_generate_source_data_file_for_size_pyspark(
            data_description=size,
            spark_session=spark_session,
        )
