
from typing import Iterable, List, Tuple

from pyspark.sql import Row

from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters)
from challenges.deduplication.domain_logic.dedupe_domain_methods import (
    BlockingFunction, CombineRowList, IsMatch)
from utils.tidy_spark_session import TidySparkSession


def dedupe_pyspark_rdd_map_part(
        spark_session: TidySparkSession,
        data_params: ExecutionParameters,
        data_set: DataSet,
):
    dfSrc = data_set.df

    rdd = (
        dfSrc.rdd
        .keyBy(BlockingFunction)
        .partitionBy(data_set.grouped_num_partitions)
        .mapPartitions(core_mappart)
    )
    return rdd, None


def AddRowToRowList(
        rows: List[Row],
        jrow: Row
) -> Iterable[Row]:
    found = False
    for index, irow in enumerate(rows):
        if not IsMatch(
                irow.FirstName, jrow.FirstName,
                irow.LastName, jrow.LastName,
                irow.ZipCode, jrow.ZipCode,
                irow.SecretKey, jrow.SecretKey):
            continue
        rows[index] = CombineRowList([irow, jrow])
        found = True
        break
    if not found:
        rows.append(jrow)
    return rows


def core_mappart(
        iterator: Iterable[Tuple[int, Row]],
) -> Iterable[Row]:
    store = {}
    for kv in iterator:
        key, row = kv
        bucket = store[key] if key in store else []
        store[key] = AddRowToRowList(bucket, row)
    for bucket in store.values():
        for row in bucket:
            yield row
