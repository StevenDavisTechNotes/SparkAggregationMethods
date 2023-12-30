import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql import Window

from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters)
from challenges.deduplication.domain_logic.dedupe_domain_methods import \
    udfMatchSingleName
from utils.spark_helpers import dfZipWithIndex
from utils.tidy_spark_session import TidySparkSession


def dedupe_pyspark_df_window(
        spark_session: TidySparkSession,
        data_params: ExecutionParameters,
        data_set: DataSet,
):
    dfSrc = data_set.df

    numPartitions = max(
        4 * data_params.NumExecutors,  # cross product
        data_set.grouped_num_partitions)
    df = dfZipWithIndex(dfSrc, spark=spark_session.spark, colName="RowId")
    df = df \
        .withColumn("BlockingKey",
                    func.hash(
                        df.ZipCode.cast(DataTypes.IntegerType()),
                        func.substring(df.FirstName, 1, 1),
                        func.substring(df.LastName, 1, 1)))
    dfBlocked = df \
        .repartition(numPartitions, df.BlockingKey)

    df1 = dfBlocked
    df2 = dfBlocked.select("RowId", "FirstName", "LastName",
                           "BlockingKey", "SecretKey").alias("df2")
    df = df1.alias("df1").join(df2, on="BlockingKey")\
        .filter((func.col("df1.RowId") == func.col("df2.RowId")) | (
            udfMatchSingleName(
                func.col("df1.FirstName"), func.col("df2.FirstName"),
                func.col("df1.SecretKey"), func.col("df2.SecretKey")) &
            udfMatchSingleName(
                func.col("df1.LastName"), func.col("df2.LastName"),
                func.col("df1.SecretKey"), func.col("df2.SecretKey"))))
    df = df \
        .withColumn("ImmediateGroupId",
                    func.least(func.col("df1.RowId"), func.col("df2.RowId")))

    window = Window \
        .partitionBy(df.BlockingKey, func.col("df1.RowId"))
    df = df \
        .withColumn("GroupId",
                    func.min(df.ImmediateGroupId).over(window)) \
        .drop(df.ImmediateGroupId) \
        .filter(func.col("df1.RowId") == func.col("df2.RowId"))
    df = df \
        .select(
            func.col('df1.BlockingKey').alias('BlockingKey'),
            func.col('df1.RowId').alias('RowId'),
            func.col('df1.FirstName').alias('FirstName'),
            func.col('df1.LastName').alias('LastName'),
            func.col('df1.StreetAddress').alias('StreetAddress'),
            func.col('df1.City').alias('City'),
            func.col('df1.ZipCode').alias('ZipCode'),
            func.col('df1.SecretKey').alias('SecretKey'),
            func.col('df1.FieldA').alias('FieldA'),
            func.col('df1.FieldB').alias('FieldB'),
            func.col('df1.FieldC').alias('FieldC'),
            func.col('df1.FieldD').alias('FieldD'),
            func.col('df1.FieldE').alias('FieldE'),
            func.col('df1.FieldF').alias('FieldF'),
            func.col('df1.SourceId').alias('SourceId'),
            df.GroupId) \
        .repartition(numPartitions, df.GroupId)
    df = df \
        .withColumn("NumNames",
                    func.when(df.FirstName.isNull(), 0)
                        .when(func.length(df.FirstName) > 0, 1)
                        .otherwise(0) +
                    func.when(df.LastName.isNull(), 0)
                        .when(func.length(df.LastName) > 0, 2)  # precedence
                        .otherwise(0)) \
        .withColumn("NumAddressParts",
                    func.when(df.StreetAddress.isNull(), 0)
                        .when(func.length(df.StreetAddress) > 0, 1)
                        .otherwise(0) +
                    func.when(df.City.isNull(), 0)
                        .when(func.length(df.City) > 0, 1)
                        .otherwise(0) +
                    func.when(df.ZipCode.isNull(), 0)
                        .when(func.length(df.ZipCode) > 0, 1)
                        .otherwise(0))
    window = Window \
        .partitionBy(df.GroupId) \
        .orderBy(df.NumNames.desc(), df.LastName.asc(), df.FirstName.asc())
    df = df \
        .withColumn("RowIdBestName", func.first(df.RowId).over(window))
    window = Window \
        .partitionBy(df.GroupId) \
        .orderBy(df.NumAddressParts.desc(), df.LastName.asc(), df.FirstName.asc())
    df = df \
        .withColumn("RowIdBestAddr", func.first(df.RowId).over(window))
    df = df \
        .groupBy(df.GroupId) \
        .agg(
            func.max(func.when(
                df.RowId == df.RowIdBestName, df.FirstName))
            .alias("FirstName"),
            func.max(func.when(
                df.RowId == df.RowIdBestName, df.LastName))
            .alias("LastName"),
            func.max(func.when(
                df.RowId == df.RowIdBestAddr, df.StreetAddress))
            .alias("StreetAddress"),
            func.max(func.when(
                df.RowId == df.RowIdBestAddr, df.City))
            .alias("City"),
            func.max(func.when(
                df.RowId == df.RowIdBestAddr, df.ZipCode))
            .alias("ZipCode"),
            func.max(df.SecretKey).alias("SecretKey"),
            func.min(df.FieldA).alias("FieldA"),
            func.min(df.FieldB).alias("FieldB"),
            func.min(df.FieldC).alias("FieldC"),
            func.min(df.FieldD).alias("FieldD"),
            func.min(df.FieldE).alias("FieldE"),
            func.min(df.FieldF).alias("FieldF")) \
        .drop(df.GroupId) \
        .repartition(2 * data_params.NumExecutors)
    return None, df
