from itertools import chain

from pyspark import RDD
from pyspark.sql import Row

from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters, TChallengePendingAnswerPythonPyspark)
from challenges.deduplication.domain_logic.dedupe_domain_methods import (
    blocking_function, combine_row_list, is_match)
from utils.tidy_spark_session import TidySparkSession


def dedupe_pyspark_rdd_reduce(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSet,
) -> TChallengePendingAnswerPythonPyspark:
    if data_set.data_size > 502000:
        return "infeasible"
    dfSrc = data_set.df
    numPartitions = data_set.grouped_num_partitions
    appendRowToList = append_row_to_list_disjoint \
        if exec_params.CanAssumeNoDupesPerPartition \
        else append_row_to_list_mixed
    rdd2: RDD[tuple[int, Row]] = \
        dfSrc.rdd \
        .keyBy(blocking_function)
    rdd3: RDD[tuple[int, list[Row]]] = rdd2 \
        .combineByKey(
            lambda x: [x],
            appendRowToList,
            combine_row_lists,
            numPartitions)
    rdd4: RDD[Row] = rdd3 \
        .mapPartitionsWithIndex(
            lambda index, iterator: chain.from_iterable(map(
                lambda kv: (x for x in kv[1]),
                iterator
            ))
    )
    return rdd4


def append_row_to_list_disjoint(
        l_rows: list[Row],
        r_row: Row,
) -> list[Row]:
    l_rows.append(r_row)
    return l_rows


def append_row_to_list_mixed(
        l_rows: list[Row],
        r_row: Row,
) -> list[Row]:
    n_initial_l_rows = len(l_rows)  # no need to test for matches in r
    found = False
    for l_index in range(0, n_initial_l_rows):
        l_row = l_rows[l_index]
        if not is_match(
                l_row.FirstName, r_row.FirstName,
                l_row.LastName, r_row.LastName,
                l_row.ZipCode, r_row.ZipCode,
                l_row.SecretKey, r_row.SecretKey):
            continue
        l_rows[l_index] = combine_row_list([l_row, r_row])
        found = True
        break
    if not found:
        l_rows.append(r_row)
    return l_rows


def combine_row_lists(
        l_rows: list[Row],
        r_rows: list[Row],
) -> list[Row]:
    n_initial_l_rows = len(l_rows)  # no need to test for matches in r
    for r_row in r_rows:
        found = False
        for l_l_index in range(0, n_initial_l_rows):
            l_row = l_rows[l_l_index]
            if not is_match(
                    l_row.FirstName, r_row.FirstName,
                    l_row.LastName, r_row.LastName,
                    l_row.ZipCode, r_row.ZipCode,
                    l_row.SecretKey, r_row.SecretKey):
                continue
            l_rows[l_l_index] = combine_row_list([l_row, r_row])
            found = True
            break
        if not found:
            l_rows.append(r_row)
    return l_rows
