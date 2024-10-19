# cSpell: ignore Plaineville
import hashlib
import os
from functools import reduce
from pathlib import Path

import pyspark.sql.functions as func
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import SparkSession
from spark_agg_methods_common_python.utils.utils import always_true, int_divide_round_up
from terminology import in_red

from src.challenges.deduplication.dedupe_test_data_types import (
    DedupeDataSetDescription, DedupePySparkDataSet, ExecutionParameters, RecordSparseStruct,
)

MAX_EXPONENT = 5
DATA_SIZE_LIST_DEDUPE = [
    DedupeDataSetDescription(
        num_people=num_people,
        num_b_recs=num_b_recs,
        num_sources=num_sources,
    )
    for num_people in [10**x for x in range(0, MAX_EXPONENT + 1)]
    for num_sources in [2, 3, 6]
    if always_true(num_b_recs := max(1, 2 * num_people // 100))
]
MAX_DATA_POINTS_PER_PARTITION: int = 10000


def derive_file_path(
        root_path: str,
        source_code: str,
        num_people: int,
) -> str:
    return root_path + "/Dedupe_Field%s%d.csv" % (source_code, num_people)


def name_hash(
        i: int,
) -> str:
    return hashlib.sha512(str(i).encode('utf8')).hexdigest()


def line(
        i: int,
        misspelledLetter: str,
) -> str:
    letter = misspelledLetter
    v = f"""
FFFFFF{letter}{i}_{name_hash(i)},
LLLLLL{letter}{i}_{name_hash(i)},
{i} Main St,Plaineville ME,
{(i-1) % 100:05d},
{i},
{i*2 if letter == "A" else ''},
{i*3 if letter == "B" else ''},
{i*5 if letter == "C" else ''},
{i*7 if letter == "D" else ''},
{i*11 if letter == "E" else ''},
{i*13 if letter == "F" else ''}
"""
    v = v.replace("\n", "") + "\n"
    return v


def generate_test_data(
    data_size_code_list: list[str],
    spark: SparkSession,
    exec_params: ExecutionParameters
) -> list[DedupePySparkDataSet]:
    root_path = os.path.join(
        exec_params.test_data_folder_location, "Dedupe_Test_Data")
    source_codes = ['A', 'B', 'C', 'D', 'E', 'F']

    Path(root_path).mkdir(parents=True, exist_ok=True)
    all_data_sets: list[DedupePySparkDataSet] = []
    target_data_size_list = [x for x in DATA_SIZE_LIST_DEDUPE if x.size_code in data_size_code_list]
    for target_data_size in sorted(target_data_size_list, key=lambda x: x.num_people):
        num_people = target_data_size.num_people
        generate_data_files(root_path, source_codes, num_people)
        single_source_data_frames: list[PySparkDataFrame]
        if exec_params.can_assume_no_dupes_per_partition:
            single_source_data_frames = [
                (spark.read
                 .csv(
                     derive_file_path(root_path, source_code, num_people),
                     schema=RecordSparseStruct)
                 .coalesce(1)
                 .withColumn("SourceId", func.lit(i_source)))
                for i_source, source_code in enumerate(source_codes)
            ]
        else:
            single_source_data_frames = [
                (spark.read
                 .csv(
                     derive_file_path(root_path, source_code, num_people),
                     schema=RecordSparseStruct)
                 .withColumn("SourceId", func.lit(i_source)))
                for i_source, source_code in enumerate(source_codes)
            ]

        def combine_sources(num: int) -> PySparkDataFrame:
            return reduce(
                lambda dfA, dfB: dfA.unionAll(dfB),
                [single_source_data_frames[i] for i in range(num)]
            )
        quantized_data_sets = {
            2: combine_sources(2),
            3: combine_sources(3),
            6: combine_sources(6),
        }
        if exec_params.can_assume_no_dupes_per_partition is False:
            # then scramble
            quantized_data_sets = {
                k: df.repartition(exec_params.num_executors)
                for k, df in quantized_data_sets.items()
            }
        for df in quantized_data_sets.values():
            df.persist()
        df = quantized_data_sets[target_data_size.num_sources]
        actual_data_size = df.count()
        if actual_data_size != target_data_size.num_source_rows:
            print(in_red(
                f"Bad result for {target_data_size.num_people}, "
                f"{target_data_size.num_sources} sources, expected "
                f"{target_data_size.num_source_rows}, got {actual_data_size}!"))
            exit(11)
        num_partitions = max(
            exec_params.default_parallelism,
            int_divide_round_up(
                actual_data_size,
                MAX_DATA_POINTS_PER_PARTITION))
        all_data_sets.append(DedupePySparkDataSet(
            data_description=target_data_size,
            grouped_num_partitions=num_partitions,
            df=df,
        ))
    return all_data_sets


def generate_data_files(
        root_path: str,
        source_codes: list[str],
        num_people: int,
) -> None:
    for source_code in source_codes:
        file_path = derive_file_path(root_path, source_code, num_people)
        if not os.path.isfile(file_path):
            num_people_to_represent = (
                num_people
                if source_code != 'B' else
                max(1, 2 * num_people // 100)
            )
            with open(file_path, "w") as f:
                for i in range(1, num_people_to_represent + 1):
                    f.write(line(i, source_code))
