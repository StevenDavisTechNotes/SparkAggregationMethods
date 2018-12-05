package DedupePerfTest

import org.apache.spark.sql.{DataFrame, SparkSession, Row, Column}
import org.apache.spark.sql.types.{IntegerType}
import org.apache.spark.sql.functions.{substring, explode, col, hash, collect_list, struct, concat}

object Blocking {
  def NestBlocksDataframe(dfIn: DataFrame): DataFrame = {
    import dfIn.sqlContext.implicits._
    var df = dfIn
    df = df
      .withColumn("BlockingKey", 
        concat(
          ($"ZipCode"),
          substring($"FirstName", 1, 1),
          substring($"LastName", 1, 1)))
    val cols = df.columns.filter(_ != "BlockingKey").map(x=>col(x))
    val allColRow = struct(cols : _*)
    df = df
      .groupBy($"BlockingKey")
        .agg(collect_list(allColRow)
            .alias("BlockedData"))
    df
  }

  def UnnestBlocksDataframe(dfIn: DataFrame): DataFrame = {
    import dfIn.sqlContext.implicits._
    var df = dfIn
    df = df
      .select(explode($"MergedItems").alias("Rows"))
      .select(col("Rows.*"))
      .drop($"BlockingKey")
      .drop($"SourceId")
    return df
  }

  def BlockingFunctionWSrc(x: RecordWSrc) = {
    x.ZipCode ++ (x.FirstName(0) ::  x.LastName(0) :: Nil)
  }
  
  def BlockingFunction(x: Record) = {
    x.ZipCode ++ (x.FirstName(0) ::  x.LastName(0) :: Nil)
  }

}