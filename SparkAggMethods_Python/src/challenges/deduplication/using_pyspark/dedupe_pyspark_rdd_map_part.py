from typing import Iterable

from pyspark.sql import Row

from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters, TChallengePendingAnswerPythonPyspark)
from challenges.deduplication.domain_logic.dedupe_domain_methods import (
    blocking_function, combine_row_list, is_match)
from utils.tidy_spark_session import TidySparkSession


def dedupe_pyspark_rdd_map_part(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSet,
) -> TChallengePendingAnswerPythonPyspark:
    if data_set.data_size > 502000:
        return "infeasible"
    dfSrc = data_set.df

    rdd = (
        dfSrc.rdd
        .keyBy(blocking_function)
        .partitionBy(data_set.grouped_num_partitions)
        .mapPartitions(core_mappart)
    )
    return rdd


def add_row_to_row_list(
        rows: list[Row],
        j_row: Row
) -> Iterable[Row]:
    found = False
    for index, i_row in enumerate(rows):
        if not is_match(
                i_row.FirstName, j_row.FirstName,
                i_row.LastName, j_row.LastName,
                i_row.ZipCode, j_row.ZipCode,
                i_row.SecretKey, j_row.SecretKey):
            continue
        rows[index] = combine_row_list([i_row, j_row])
        found = True
        break
    if not found:
        rows.append(j_row)
    return rows


def core_mappart(
        iterator: Iterable[tuple[int, Row]],
) -> Iterable[Row]:
    store = {}
    for kv in iterator:
        key, row = kv
        bucket = store[key] if key in store else []
        store[key] = add_row_to_row_list(bucket, row)
    for bucket in store.values():
        for row in bucket:
            yield row
