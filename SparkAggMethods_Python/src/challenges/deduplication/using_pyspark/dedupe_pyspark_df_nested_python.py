import pyspark.sql.functions as func

from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters, PysparkPythonPendingAnswerSet)
from challenges.deduplication.domain_logic.dedupe_domain_methods import (
    SinglePass_RecList_DF_Returns, nest_blocks_dataframe, single_pass_rec_list,
    unnest_blocks_dataframe)
from utils.tidy_spark_session import TidySparkSession


def dedupe_pyspark_df_nested_python(
        spark_session: TidySparkSession,
        data_params: ExecutionParameters,
        data_set: DataSet,
) -> PysparkPythonPendingAnswerSet:
    if data_set.data_size > 502000:
        return PysparkPythonPendingAnswerSet(feasible=False)

    dfSrc = data_set.df
    df = nest_blocks_dataframe(dfSrc, data_set.grouped_num_partitions)
    df = df \
        .withColumn("MergedItems",
                    func.udf(single_pass_rec_list,
                             SinglePass_RecList_DF_Returns)(
                        df.BlockedData))
    df = unnest_blocks_dataframe(df)
    return PysparkPythonPendingAnswerSet(spark_df=df)
