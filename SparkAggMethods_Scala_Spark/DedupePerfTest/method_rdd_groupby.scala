package DedupePerfTest

import scala.collection.{ mutable }
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset, Row }
import org.apache.spark.sql.functions.{ udf }
import DedupePerfTest._
import scala.reflect.api.materializeTypeTag

object method_rdd_groupby {
  def run(
    generatedDataSet: GeneratedDataSet,
    NumExecutors:     Int,
    canAssumeNoDupesPerPartition: Boolean,
    spark:            SparkSession): (RDD[Record], DataFrame, Dataset[Record]) = {
    val fncSingle: (mutable.WrappedArray[Row]) => List[Record] =
      (blockedDataAsRows: mutable.WrappedArray[Row]) => {
        var blockedData = blockedDataAsRows.map(RecordMethods.rowToRecordWSrc).toArray
        var firstOrderEdges = Matching.FindRecordMatches_RecList(blockedData)
        val connectedComponents = Matching.FindConnectedComponents_RecList(firstOrderEdges)
        firstOrderEdges = null
        val mergedItems = Matching.MergeItems_RecList(blockedData, connectedComponents)
        mergedItems
      };
    var rdd = generatedDataSet.rddWSrc
      .groupBy(Blocking.BlockingFunctionWSrc _, numPartitions=NumExecutors)
      .flatMap(p => Matching.ProcessBlock(p._2))
    (rdd, null, null)
  }
}