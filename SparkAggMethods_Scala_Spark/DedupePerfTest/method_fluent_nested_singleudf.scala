package DedupePerfTest

import scala.collection.{ mutable }
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset, Row }
import org.apache.spark.sql.functions.{ udf }
//import scala.reflect.api.materializeTypeTag

object method_fluent_nested_singleudf {
  def run(
    generatedDataSet: GeneratedDataSet,
    NumExecutors:     Int,
    canAssumeNoDupesPerPartition: Boolean,
    spark:            SparkSession): 
    (RDD[Record], DataFrame, 
        Dataset[Record]) = {
    import spark.implicits._
    val fncSingle: 
      (mutable.WrappedArray[Row]) 
      => List[Record] =
      (blockedDataAsRows: 
          mutable.WrappedArray[Row]) => {
        var srcRecordData = 
          blockedDataAsRows
            .map(RecordMethods.rowToRecordWSrc)
        Matching.ProcessBlock(srcRecordData)
      }
    val udfSingle = udf(fncSingle)
    var df = generatedDataSet.dfWSrc
    df = Blocking.NestBlocksDataframe(df)
    df = df
      .withColumn(
        "MergedItems",
        udfSingle($"BlockedData"))
    df = Blocking.UnnestBlocksDataframe(df)
    (null, df, null)
  }
}