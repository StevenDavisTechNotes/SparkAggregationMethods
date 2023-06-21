from pyspark.sql import DataFrame as spark_DataFrame

from DedupePerfTest.DedupeDomain import (BlockingFunction, CombineRowList,
                                         IsMatch)
from DedupePerfTest.DedupeTestData import DedupeDataParameters
from Utils.SparkUtils import TidySparkSession

# region method_rdd_mappart


def method_rdd_mappart(_spark_session: TidySparkSession, data_params: DedupeDataParameters, _dataSize: int, dfSrc: spark_DataFrame):
    def AddRowToRowList(rows, jrow):
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


    def core_mappart(iterator):
        store = {}
        for kv in iterator:
            key, row = kv
            bucket = store[key] if key in store else []
            store[key] = AddRowToRowList(bucket, row)
        for bucket in store.values():
            for row in bucket:
                yield row

    rdd = dfSrc.rdd \
        .keyBy(BlockingFunction) \
        .partitionBy(data_params.NumExecutors) \
        .mapPartitions(core_mappart)
    return rdd, None
# endregion
