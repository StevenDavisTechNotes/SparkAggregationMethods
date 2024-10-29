import pyspark.sql.functions as func

from src.challenges.deduplication.dedupe_test_data_types_pyspark import (
    DedupeExecutionParametersPyspark, DedupePySparkDataSet, TChallengePendingAnswerPythonPyspark,
)
from src.challenges.deduplication.domain_logic.dedupe_domain_methods_pyspark import (
    FindConnectedComponents_RecList_Returns, FindRecordMatches_RecList_Returns, MergeItems_RecList_Returns,
    find_connected_components_rec_list, find_record_matches_rec_list, merge_items_rec_list, nest_blocks_dataframe,
    unnest_blocks_dataframe,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def dedupe_pyspark_df_nested_w_col(
        spark_session: TidySparkSession,
        exec_params: DedupeExecutionParametersPyspark,
        data_set: DedupePySparkDataSet,
) -> TChallengePendingAnswerPythonPyspark:
    if data_set.data_description.num_source_rows > 20200:
        return "infeasible"
    dfSrc = data_set.df
    df = nest_blocks_dataframe(dfSrc, data_set.grouped_num_partitions)
    df = df \
        .withColumn("FirstOrderEdges",
                    func.udf(find_record_matches_rec_list,
                             FindRecordMatches_RecList_Returns)(
                        df.BlockedData))
    df = df \
        .withColumn("ConnectedComponents",
                    func.udf(find_connected_components_rec_list,
                             FindConnectedComponents_RecList_Returns)(
                        df.FirstOrderEdges)) \
        .drop(df.FirstOrderEdges)
    df = df \
        .withColumn("MergedItems",
                    func.udf(merge_items_rec_list,
                             MergeItems_RecList_Returns)(
                        df.BlockedData, df.ConnectedComponents))
    df = unnest_blocks_dataframe(df)
    return df
