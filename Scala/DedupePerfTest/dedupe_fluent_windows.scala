package DedupePerfTest

import org.apache.log4j.Logger
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SparkSession, Row, DataFrame, Dataset }
import org.apache.spark.sql.expressions.{ Window }
import org.apache.spark.sql.{ functions => func }
import org.apache.spark.sql.functions.{ col, when, lit }
import org.apache.spark.sql.types.{ IntegerType, BooleanType }
import Common.ZipWithIndex

object dedupe_fluent_windows {
  def run(
    generatedDataSet:             GeneratedDataSet,
    NumExecutors:                 Int,
    canAssumeNoDupesPerPartition: Boolean,
    spark:                        SparkSession): (RDD[Record], DataFrame, Dataset[Record]) = {
    import spark.implicits._
    val numPartitions = 4 * NumExecutors // cross product
    def addIndex(row: RecordWSrc, rowId: Long): RecordWSrcWRowId = {
      RecordWSrcWRowId(
        FirstName = row.FirstName,
        LastName = row.LastName,
        StreetAddress = row.StreetAddress,
        City = row.City,
        ZipCode = row.ZipCode,
        SecretKey = row.SecretKey,
        FieldA = row.FieldA,
        FieldB = row.FieldB,
        FieldC = row.FieldC,
        FieldD = row.FieldD,
        FieldE = row.FieldE,
        FieldF = row.FieldF,
        SourceId = row.SourceId,
        RowId = rowId)
    }

    var dfWRowId = ZipWithIndex.dfZipWithIndex(
      generatedDataSet.dfWSrc, offset = 1,
      colName = "RowId")

    var dfBlocked: DataFrame = null;
    { // block data
      var df = dfWRowId
      df = df
        .withColumn(
          "BlockingKey",
          org.apache.spark.sql.functions.hash(
            $"ZipCode".cast(IntegerType),
            func.substring($"FirstName", 1, 1),
            func.substring($"LastName", 1, 1)))
      dfBlocked = df
        .repartition(numPartitions, $"BlockingKey")
    }

    var dfWImmediateGroupId: DataFrame = null;
    { // construct ImmediateGroupId
      val df1 = dfBlocked.alias("df1")
      val df2 = dfBlocked
        .select("RowId", "FirstName",
          "LastName", "BlockingKey", "SecretKey")
        .alias("df2")
      val udfMatchSingleName_1 = func.udf(
        Matching.MatchSingleName _,
        Matching.MatchSingleName_Returns)
      val udfMatchSingleName = func.udf(
        Matching.MatchSingleName _)

      var df = df1
        .join(
          df2,
          $"df1.BlockingKey" === $"df2.BlockingKey",
          "inner")
      df = df
        .drop($"df2.BlockingKey")
        .withColumnRenamed(
          "df1.BlockingKey",
          "BlockingKey")
      df = df
        .filter(($"df1.RowId" === $"df2.RowId").or(
          udfMatchSingleName(
            $"df1.FirstName", $"df2.FirstName",
            $"df1.SecretKey", $"df2.SecretKey").and(
              udfMatchSingleName(
                $"df1.LastName", $"df2.LastName",
                $"df1.SecretKey", $"df2.SecretKey"))))
      df = df
        .withColumn(
          "ImmediateGroupId",
          func.least(
            col("df1.RowId"),
            col("df2.RowId")))
      df = df
        .select(
          $"df1.BlockingKey".alias("BlockingKey"),
          $"df1.RowId".alias("RowId"),
          $"df1.FirstName".alias("FirstName"),
          $"df1.LastName".alias("LastName"),
          $"df1.StreetAddress".alias("StreetAddress"),
          $"df1.City".alias("City"),
          $"df1.ZipCode".alias("ZipCode"),
          $"df1.SecretKey".alias("SecretKey"),
          $"df1.FieldA".alias("FieldA"),
          $"df1.FieldB".alias("FieldB"),
          $"df1.FieldC".alias("FieldC"),
          $"df1.FieldD".alias("FieldD"),
          $"df1.FieldE".alias("FieldE"),
          $"df1.FieldF".alias("FieldF"),
          $"df1.SourceId".alias("SourceId"),
          $"df2.RowId".alias("RowId2"),
          $"ImmediateGroupId")
      dfWImmediateGroupId = df
    }
    var dfWGroupId: DataFrame = null;
    { // assign GroupId from ImmediateGroupId

      // The following causes a StackOverflowError
      // no matter how large I set the stack
      // df.withColumn(
      //   "GroupId",
      //   func.min(
      //     $"ImmediateGroupId".over(Window
      //     .partitionBy(
      //       $"BlockingKey",
      //       $"RowId1"))))
      // so I am using SQL as a workaround for Scala
      dfWImmediateGroupId.createTempView(
        "dfWImmediateGroupId")
      var df = spark.sql("""
      SELECT *, 
        MIN(ImmediateGroupId) 
          OVER (PARTITION BY RowId) 
          as GroupId 
      FROM dfWImmediateGroupId
      """)
      spark.catalog.dropTempView("dfWImmediateGroupId")

      df = df
        .drop($"ImmediateGroupId")
        .filter($"RowId" === $"RowId2")
        .drop($"RowId2")
      df = df
        .repartition(numPartitions, $"GroupId")
      dfWGroupId = df
    }
    var dfBestAddress: DataFrame = null;
    { // identify how complete addresses and names are
      var df = dfWGroupId
      df = df
        .withColumn(
          "NumNames",
          when($"FirstName".isNull, 0)
            .when(func.length($"FirstName") > 0, 1)
            .otherwise(0) +
            when($"LastName".isNull, 0)
            .when(func.length($"LastName") > 0, 2)
            .otherwise(0))
        .withColumn(
          "NumAddressParts",
          when($"StreetAddress".isNull, 0)
            .when(func.length($"StreetAddress") > 0, 1)
            .otherwise(0) +
            when($"City".isNull, 0)
            .when(func.length($"City") > 0, 1)
            .otherwise(0) +
            when($"ZipCode".isNull, 0)
            .when(func.length($"ZipCode") > 0, 1)
            .otherwise(0))
      df = df
        .withColumn(
          "RowIdBestName",
          func.first($"RowId").over(
            Window
              .partitionBy($"GroupId")
              .orderBy(
                $"NumNames".desc,
                $"LastName".asc,
                $"FirstName".asc)))
      df = df
        .withColumn("RowIdBestAddr", func.first($"RowId").over(
          Window
            .partitionBy($"GroupId")
            .orderBy(
              $"NumAddressParts".desc,
              $"LastName".asc,
              $"FirstName".asc)))
      dfBestAddress = df
    }
    var dfSelected: DataFrame = null;
    { // pick be the best values within the group
      var df = dfBestAddress
      df = df
        .groupBy($"GroupId")
        .agg(
          func.max(
            when($"RowId" === $"RowIdBestName", $"FirstName"))
            .alias("FirstName"),
          func.max(
            when($"RowId" === $"RowIdBestName", $"LastName"))
            .alias("LastName"),
          func.max(
            when($"RowId" === $"RowIdBestAddr", $"StreetAddress"))
            .alias("StreetAddress"),
          func.max(
            when($"RowId" === $"RowIdBestAddr", $"City"))
            .alias("City"),
          func.max(
            when($"RowId" === $"RowIdBestAddr", $"ZipCode"))
            .alias("ZipCode"),
          func.max($"SecretKey").alias("SecretKey"),
          func.min($"FieldA").alias("FieldA"),
          func.min($"FieldB").alias("FieldB"),
          func.min($"FieldC").alias("FieldC"),
          func.min($"FieldD").alias("FieldD"),
          func.min($"FieldE").alias("FieldE"),
          func.min($"FieldF").alias("FieldF"))
        .drop($"GroupId")
      dfSelected = df
    }
    return (null, dfSelected, null)
  }
}