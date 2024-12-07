from typing import Iterable

from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.deduplication.domain_logic.dedupe_domain_methods import (
    is_match,
)

from src.challenges.deduplication.dedupe_test_data_types_pyspark import (
    DedupeDataSetPySpark, DedupeExecutionParametersPyspark,
    TChallengePendingAnswerPythonPyspark,
)
from src.challenges.deduplication.domain_logic.dedupe_domain_methods_pyspark import (
    blocking_function, combine_row_list,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def dedupe_pyspark_rdd_map_part(
        spark_session: TidySparkSession,
        exec_params: DedupeExecutionParametersPyspark,
        data_set: DedupeDataSetPySpark,
) -> TChallengePendingAnswerPythonPyspark:
    if data_set.data_description.num_source_rows > 502000:
        return "infeasible", "Unknown reason"
    dfSrc = data_set.df_source

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
