from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..DedupeDomain import BlockingFunction, SinglePass_RecList
from ..DedupeDataTypes import DataSetOfSizeOfSources, ExecutionParameters, RecordSparseStruct

# region method_rdd_groupby


def method_rdd_groupby(
    spark_session: TidySparkSession,
    data_params: ExecutionParameters,
    data_set: DataSetOfSizeOfSources,
):
    dfSrc = data_set.df

    numPartitions = data_params.NumExecutors
    rdd = dfSrc.rdd \
        .groupBy(BlockingFunction, numPartitions) \
        .flatMapValues(lambda iter:
                       SinglePass_RecList(list(iter))) \
        .values()
    return rdd, None


# endregion
