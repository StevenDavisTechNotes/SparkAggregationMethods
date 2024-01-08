from typing import Iterable

from pyspark.sql import Row

from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters, PysparkPythonPendingAnswerSet)
from challenges.deduplication.domain_logic.dedupe_domain_methods import (
    blocking_function, combine_row_list, is_match)
from utils.tidy_spark_session import TidySparkSession


def dedupe_pyspark_rdd_map_part(
        spark_session: TidySparkSession,
        data_params: ExecutionParameters,
        data_set: DataSet,
) -> PysparkPythonPendingAnswerSet:
    if data_set.data_size > 502000:
        return PysparkPythonPendingAnswerSet(feasible=False)
    dfSrc = data_set.df

    rdd = (
        dfSrc.rdd
        .keyBy(blocking_function)
        .partitionBy(data_set.grouped_num_partitions)
        .mapPartitions(core_mappart)
    )
    return PysparkPythonPendingAnswerSet(rdd_row=rdd)


def add_row_to_row_list(
        rows: list[Row],
        jrow: Row
) -> Iterable[Row]:
    found = False
    for index, irow in enumerate(rows):
        if not is_match(
                irow.FirstName, jrow.FirstName,
                irow.LastName, jrow.LastName,
                irow.ZipCode, jrow.ZipCode,
                irow.SecretKey, jrow.SecretKey):
            continue
        rows[index] = combine_row_list([irow, jrow])
        found = True
        break
    if not found:
        rows.append(jrow)
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
