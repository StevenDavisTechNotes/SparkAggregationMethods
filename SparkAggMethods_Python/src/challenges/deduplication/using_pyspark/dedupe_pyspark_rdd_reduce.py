from itertools import chain

from pyspark import RDD
from pyspark.sql import Row

from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters, PysparkPythonPendingAnswerSet)
from challenges.deduplication.domain_logic.dedupe_domain_methods import (
    blocking_function, combine_row_list, is_match)
from utils.tidy_spark_session import TidySparkSession


def dedupe_pyspark_rdd_reduce(
        _spark_session: TidySparkSession,
        data_params: ExecutionParameters,
        data_set: DataSet,
) -> PysparkPythonPendingAnswerSet:
    if data_set.data_size > 502000:
        return PysparkPythonPendingAnswerSet(feasible=False)
    dfSrc = data_set.df
    numPartitions = data_set.grouped_num_partitions
    appendRowToList = append_row_to_list_disjoint \
        if data_params.CanAssumeNoDupesPerPartition \
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
    return PysparkPythonPendingAnswerSet(rdd_row=rdd4)


def append_row_to_list_disjoint(
        lrows: list[Row],
        rrow: Row,
) -> list[Row]:
    lrows.append(rrow)
    return lrows


def append_row_to_list_mixed(
        lrows: list[Row],
        rrow: Row,
) -> list[Row]:
    nInitialLRows = len(lrows)  # no need to test for matches in r
    found = False
    for lindex in range(0, nInitialLRows):
        lrow = lrows[lindex]
        if not is_match(
                lrow.FirstName, rrow.FirstName,
                lrow.LastName, rrow.LastName,
                lrow.ZipCode, rrow.ZipCode,
                lrow.SecretKey, rrow.SecretKey):
            continue
        lrows[lindex] = combine_row_list([lrow, rrow])
        found = True
        break
    if not found:
        lrows.append(rrow)
    return lrows


def combine_row_lists(
        lrows: list[Row],
        rrows: list[Row],
) -> list[Row]:
    nInitialLRows = len(lrows)  # no need to test for matches in r
    for rindex, rrow in enumerate(rrows):
        found = False
        for lindex in range(0, nInitialLRows):
            lrow = lrows[lindex]
            if not is_match(
                    lrow.FirstName, rrow.FirstName,
                    lrow.LastName, rrow.LastName,
                    lrow.ZipCode, rrow.ZipCode,
                    lrow.SecretKey, rrow.SecretKey):
                continue
            lrows[lindex] = combine_row_list([lrow, rrow])
            found = True
            break
        if not found:
            lrows.append(rrow)
    return lrows
