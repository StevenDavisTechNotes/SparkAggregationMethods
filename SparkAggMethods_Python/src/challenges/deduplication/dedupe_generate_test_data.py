# cSpell: ignore Plaineville
import hashlib
import os
from dataclasses import dataclass
from functools import reduce
from pathlib import Path

import pyspark.sql.functions as func
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import SparkSession

from src.challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters, RecordSparseStruct)
from src.utils.utils import always_true, int_divide_round_up


@dataclass(frozen=True)
class DataSetDescription:
    size_code: str
    num_people: int
    num_b_recs: int
    num_sources: int
    data_size: int


MAX_EXPONENT = 5
DATA_SIZE_LIST_DEDUPE = [
    DataSetDescription(
        size_code=data_size_code,
        num_people=num_people,
        num_b_recs=num_b_recs,
        num_sources=num_sources,
        data_size=num_rows,
    )
    for num_people in [10**x for x in range(0, MAX_EXPONENT + 1)]
    for num_sources in [2, 3, 6]
    if always_true(num_b_recs := max(1, 2 * num_people // 100))
    if always_true(num_rows := (num_sources - 1) * num_people + num_b_recs)
    if always_true(logical_data_size := num_sources * num_people)
    if always_true(data_size_code :=
                   str(logical_data_size)
                   if logical_data_size < 1000 else
                   f'{logical_data_size//1000}k')
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
) -> list[DataSet]:
    root_path = os.path.join(
        exec_params.TestDataFolderLocation, "Dedupe_Test_Data")
    source_codes = ['A', 'B', 'C', 'D', 'E', 'F']

    Path(root_path).mkdir(parents=True, exist_ok=True)
    all_data_sets: list[DataSet] = []
    target_data_size_list = [x for x in DATA_SIZE_LIST_DEDUPE if x.size_code in data_size_code_list]
    for num_people in sorted({x.num_people for x in target_data_size_list}):
        generate_data_files(root_path, source_codes, num_people)
        single_source_data_frames: list[PySparkDataFrame]
        if exec_params.CanAssumeNoDupesPerPartition:
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
        if exec_params.CanAssumeNoDupesPerPartition is False:  # Scramble
            quantized_data_sets = {
                k: df.repartition(exec_params.NumExecutors)
                for k, df in quantized_data_sets.items()
            }
        for df in quantized_data_sets.values():
            df.persist()
        for num_sources, df in quantized_data_sets.items():
            data_size = df.count()
            if data_size not in [x.data_size for x in target_data_size_list]:
                continue
            num_partitions = max(
                exec_params.DefaultParallelism,
                int_divide_round_up(
                    data_size,
                    MAX_DATA_POINTS_PER_PARTITION))
            all_data_sets.append(DataSet(
                num_people=num_people,
                num_sources=num_sources,
                data_size=data_size,
                grouped_num_partitions=num_partitions,
                df=df))
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
