import pyspark.sql.functions as func
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..DedupeDomain import (
    NestBlocksDataframe, SinglePass_RecList, SinglePass_RecList_DF_Returns, UnnestBlocksDataframe)
from ..DedupeDataTypes import DataSetOfSizeOfSources, ExecutionParameters, RecordSparseStruct


def dedupe_fluent_nested_python(
    spark_session: TidySparkSession,
    data_params: ExecutionParameters,
    data_set: DataSetOfSizeOfSources,
):
    dfSrc = data_set.df
    df = NestBlocksDataframe(dfSrc, data_set.grouped_num_partitions)
    df = df \
        .withColumn("MergedItems",
                    func.udf(SinglePass_RecList,
                             SinglePass_RecList_DF_Returns)(
                        df.BlockedData))
    df = UnnestBlocksDataframe(df)
    return None, df
