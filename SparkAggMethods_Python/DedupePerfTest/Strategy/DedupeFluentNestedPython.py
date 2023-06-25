from pyspark.sql import DataFrame as spark_DataFrame
import pyspark.sql.functions as func

from DedupePerfTest.DedupeDomain import NestBlocksDataframe, SinglePass_RecList, SinglePass_RecList_DF_Returns, UnnestBlocksDataframe
from DedupePerfTest.DedupeTestData import DedupeDataParameters
from Utils.SparkUtils import TidySparkSession

# region method_fluent_nested_python


def method_fluent_nested_python(_spark_session: TidySparkSession, _data_params: DedupeDataParameters, _dataSize: int, dfSrc: spark_DataFrame):
    df = NestBlocksDataframe(dfSrc)
    df = df \
        .withColumn("MergedItems",
                    func.udf(SinglePass_RecList,
                             SinglePass_RecList_DF_Returns)(
                        df.BlockedData))
    df = UnnestBlocksDataframe(df)
    return None, df


# endregion