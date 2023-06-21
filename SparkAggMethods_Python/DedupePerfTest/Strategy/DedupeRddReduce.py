from itertools import chain
from typing import List, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from DedupePerfTest.DedupeDomain import (BlockingFunction, CombineRowList,
                                         IsMatch)
from DedupePerfTest.DedupeTestData import DedupeDataParameters
from Utils.SparkUtils import TidySparkSession

# region method_rdd_reduce


def method_rdd_reduce(_spark_session: TidySparkSession, data_params: DedupeDataParameters, _dataSize: int, dfSrc: spark_DataFrame):
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

    numPartitions = data_params.NumExecutors
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


# endregion
