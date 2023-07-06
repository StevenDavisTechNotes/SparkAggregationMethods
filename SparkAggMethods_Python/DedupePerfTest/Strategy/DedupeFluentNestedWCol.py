import pyspark.sql.functions as func
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..DedupeDomain import (
    FindConnectedComponents_RecList,
    FindConnectedComponents_RecList_Returns,
    FindRecordMatches_RecList,
    FindRecordMatches_RecList_Returns,
    MergeItems_RecList, MergeItems_RecList_Returns,
    NestBlocksDataframe, UnnestBlocksDataframe
)
from ..DedupeDataTypes import DataSetOfSizeOfSources, ExecutionParameters, RecordSparseStruct

# region method_fluent_nested_withCol


def method_fluent_nested_withCol(
    spark_session: TidySparkSession,
    data_params: ExecutionParameters,
    data_set: DataSetOfSizeOfSources,
):
    dfSrc = data_set.df
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
