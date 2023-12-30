import pyspark.sql.functions as func

from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters)
from challenges.deduplication.domain_logic.dedupe_domain_methods import (
    FindConnectedComponents_RecList, FindConnectedComponents_RecList_Returns,
    FindRecordMatches_RecList, FindRecordMatches_RecList_Returns,
    MergeItems_RecList, MergeItems_RecList_Returns, NestBlocksDataframe,
    UnnestBlocksDataframe)
from utils.tidy_spark_session import TidySparkSession


def dedupe_pyspark_df_nested_w_col(
        spark_session: TidySparkSession,
        data_params: ExecutionParameters,
        data_set: DataSet,
):
    dfSrc = data_set.df
    df = NestBlocksDataframe(dfSrc, data_set.grouped_num_partitions)
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
