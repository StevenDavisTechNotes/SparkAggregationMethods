
from Utils.SparkUtils import TidySparkSession

from ..DedupeDomain import BlockingFunction, CombineRowList, IsMatch
from ..DedupeDataTypes import DataSet, ExecutionParameters


def dedupe_rdd_mappart(
    spark_session: TidySparkSession,
    data_params: ExecutionParameters,
    data_set: DataSet,
):
    dfSrc = data_set.df

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

    rdd = (
        dfSrc.rdd
        .keyBy(BlockingFunction)
        .partitionBy(data_set.grouped_num_partitions)
        .mapPartitions(core_mappart)
    )
    return rdd, None
