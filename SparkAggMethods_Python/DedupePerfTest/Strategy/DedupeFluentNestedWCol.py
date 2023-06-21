import pyspark.sql.functions as func
from pyspark.sql import DataFrame as spark_DataFrame

from DedupePerfTest.DedupeDomain import (
    FindConnectedComponents_RecList, FindConnectedComponents_RecList_Returns,
    FindRecordMatches_RecList, FindRecordMatches_RecList_Returns,
    MergeItems_RecList, MergeItems_RecList_Returns, NestBlocksDataframe,
    UnnestBlocksDataframe)
from DedupePerfTest.DedupeTestData import DedupeDataParameters
from Utils.SparkUtils import TidySparkSession

# region method_fluent_nested_withCol


def method_fluent_nested_withCol(_spark_session: TidySparkSession, _data_params: DedupeDataParameters, _dataSize: int, dfSrc: spark_DataFrame):
    df = NestBlocksDataframe(dfSrc)
    df = df \
        .withColumn("FirstOrderEdges",
                    func.udf(FindRecordMatches_RecList,
                             FindRecordMatches_RecList_Returns)(
                        df.BlockedData))
    df = df \
        .withColumn("ConnectedComponents",
                    func.udf(FindConnectedComponents_RecList,
                             FindConnectedComponents_RecList_Returns)(
                        df.FirstOrderEdges)) \
        .drop(df.FirstOrderEdges)
    df = df \
        .withColumn("MergedItems",
                    func.udf(MergeItems_RecList,
                             MergeItems_RecList_Returns)(
                        df.BlockedData, df.ConnectedComponents))
    df = UnnestBlocksDataframe(df)
    return None, df


# endregion
