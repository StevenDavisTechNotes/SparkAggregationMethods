
from Utils.SparkUtils import TidySparkSession

from ..DedupeDomain import BlockingFunction, SinglePass_RecList
from ..DedupeDataTypes import DataSet, ExecutionParameters


def dedupe_rdd_groupby(
    spark_session: TidySparkSession,
    data_params: ExecutionParameters,
    data_set: DataSet,
):
    dfSrc = data_set.df

    rdd = (
        dfSrc.rdd
        .groupBy(BlockingFunction, data_set.grouped_num_partitions)
        .flatMapValues(lambda iter:
                       SinglePass_RecList(list(iter)))
        .values())
    return rdd, None
