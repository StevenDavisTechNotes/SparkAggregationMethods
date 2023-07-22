from itertools import chain
from typing import List, Tuple

from pyspark import RDD
from pyspark.sql import Row

from Utils.SparkUtils import TidySparkSession

from ..DedupeDomain import BlockingFunction, CombineRowList, IsMatch
from ..DedupeDataTypes import DataSetOfSizeOfSources, ExecutionParameters


def dedupe_rdd_reduce(
    spark_session: TidySparkSession,
    data_params: ExecutionParameters,
    data_set: DataSetOfSizeOfSources,
):
    dfSrc = data_set.df

    def appendRowToListDisjoint(lrows: List[Row], rrow: Row) -> List[Row]:
        lrows.append(rrow)
        return lrows

    def appendRowToListMixed(lrows: List[Row], rrow: Row) -> List[Row]:
        nInitialLRows = len(lrows)  # no need to test for matches in r
        found = False
        for lindex in range(0, nInitialLRows):
            lrow = lrows[lindex]
            if not IsMatch(
                    lrow.FirstName, rrow.FirstName,
                    lrow.LastName, rrow.LastName,
                    lrow.ZipCode, rrow.ZipCode,
                    lrow.SecretKey, rrow.SecretKey):
                continue
            lrows[lindex] = CombineRowList([lrow, rrow])
            found = True
            break
        if not found:
            lrows.append(rrow)
        return lrows

    def CombineRowLists(lrows: List[Row], rrows: List[Row]) -> List[Row]:
        nInitialLRows = len(lrows)  # no need to test for matches in r
        for rindex, rrow in enumerate(rrows):
            found = False
            for lindex in range(0, nInitialLRows):
                lrow = lrows[lindex]
                if not IsMatch(
                        lrow.FirstName, rrow.FirstName,
                        lrow.LastName, rrow.LastName,
                        lrow.ZipCode, rrow.ZipCode,
                        lrow.SecretKey, rrow.SecretKey):
                    continue
                lrows[lindex] = CombineRowList([lrow, rrow])
                found = True
                break
            if not found:
                lrows.append(rrow)
        return lrows

    numPartitions = data_set.grouped_num_partitions
    appendRowToList = appendRowToListDisjoint \
        if data_params.CanAssumeNoDupesPerPartition \
        else appendRowToListMixed
    rdd2: RDD[Tuple[int, Row]] = \
        dfSrc.rdd \
        .keyBy(BlockingFunction)
    rdd3: RDD[Tuple[int, List[Row]]] = rdd2 \
        .combineByKey(
            lambda x: [x],
            appendRowToList,
            CombineRowLists,
            numPartitions)
    rdd4: RDD[Row] = rdd3 \
        .mapPartitionsWithIndex(
            lambda index, iterator: chain.from_iterable(map(
                lambda kv: (x for x in kv[1]),
                iterator
            ))
    )
    return rdd4, None
