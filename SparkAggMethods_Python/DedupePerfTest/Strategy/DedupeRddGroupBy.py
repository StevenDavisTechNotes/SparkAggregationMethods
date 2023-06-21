from pyspark.sql import DataFrame as spark_DataFrame

from DedupePerfTest.DedupeDomain import (
    BlockingFunction, SinglePass_RecList)
from DedupePerfTest.DedupeTestData import DedupeDataParameters
from Utils.SparkUtils import TidySparkSession

# region method_rdd_groupby


def method_rdd_groupby(_spark_session: TidySparkSession, data_params: DedupeDataParameters, _dataSize: int, dfSrc: spark_DataFrame):
    numPartitions = data_params.NumExecutors
    rdd = dfSrc.rdd \
        .groupBy(BlockingFunction, numPartitions) \
        .flatMapValues(lambda iter:
                       SinglePass_RecList(list(iter))) \
        .values()
    return rdd, None


# endregion
