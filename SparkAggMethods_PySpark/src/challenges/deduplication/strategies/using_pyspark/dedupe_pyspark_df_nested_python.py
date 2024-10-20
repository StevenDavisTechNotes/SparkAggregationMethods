import pyspark.sql.functions as func

from src.challenges.deduplication.dedupe_test_data_types_pyspark import (
    DedupePySparkDataSet, ExecutionParameters, TChallengePendingAnswerPythonPyspark,
)
from src.challenges.deduplication.domain_logic.dedupe_domain_methods_pyspark import (
    SinglePass_RecList_DF_Returns, nest_blocks_dataframe, single_pass_rec_list, unnest_blocks_dataframe,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def dedupe_pyspark_df_nested_python(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DedupePySparkDataSet,
) -> TChallengePendingAnswerPythonPyspark:
    if data_set.data_description.num_source_rows > 502000:
        return "infeasible"

    dfSrc = data_set.df
    df = nest_blocks_dataframe(dfSrc, data_set.grouped_num_partitions)
    df = df \
        .withColumn("MergedItems",
                    func.udf(single_pass_rec_list,
                             SinglePass_RecList_DF_Returns)(
                        df.BlockedData))
    df = unnest_blocks_dataframe(df)
    return df
