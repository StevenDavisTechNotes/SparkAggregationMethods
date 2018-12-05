package DedupePerfTest

import scala.collection.{ mutable }
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset, Row }
import org.apache.spark.sql.functions.{ udf }
import DedupePerfTest._
import scala.reflect.api.materializeTypeTag

object method_fluent_nested_withCol {
  def run(
    generatedDataSet: GeneratedDataSet,
    NumExecutors:     Int,
    canAssumeNoDupesPerPartition: Boolean,
    spark:            SparkSession): (RDD[Record], DataFrame, Dataset[Record]) = {
    import spark.implicits._

    val udfFindRecordMatches = udf(FindRecordMatches_RecList _)
    val udfFindConnectedComponents_RecList = udf(FindConnectedComponents_RecList _)
    val udfMergeItems_RecList = udf(MergeItems_RecList _)
    var df = generatedDataSet.dfWSrc
    df = Blocking.NestBlocksDataframe(df)
    df = df
      .withColumn(
        "FirstOrderEdges",
        udfFindRecordMatches($"BlockedData"))
    df = df
      .withColumn(
        "ConnectedComponents",
        udfFindConnectedComponents_RecList($"FirstOrderEdges"))
    df = df.drop($"FirstOrderEdges")
    df = df
      .withColumn(
        "MergedItems",
        udfMergeItems_RecList(
          $"BlockedData", $"ConnectedComponents"))
    df = Blocking.UnnestBlocksDataframe(df)
    (null, df, null)
  }
  def FindRecordMatches_RecList(blockedDataAsRows: mutable.WrappedArray[Row]): List[Matching.Edge] = {
    var blockedData = blockedDataAsRows.map(RecordMethods.rowToRecordWSrc).toArray
    Matching.FindRecordMatches_RecList(blockedData)
  }
  def FindConnectedComponents_RecList(blockedDataAsRows: mutable.WrappedArray[Row]): List[Matching.Component] = {
    var blockedData = blockedDataAsRows.map((x: Row) => Matching.Edge(
      x.getAs[Int]("idLeftVertex"),
      x.getAs[Int]("idRightVertex")))
    Matching.FindConnectedComponents_RecList(blockedData)
  }
  def MergeItems_RecList(
    recordListAsRows:             mutable.WrappedArray[Row],
    connectedComponentListAsRows: mutable.WrappedArray[Row]): List[Record] = {
    var recordList = recordListAsRows.map(RecordMethods.rowToRecordWSrc).toArray
    var connectedComponentList = connectedComponentListAsRows.map((x: Row) => {
      val colNoIdVertexList = x.fieldIndex("idVertexList");
      val idVertextList: Array[Int] = x.getAs[List[Int]](colNoIdVertexList).toArray;
      Matching.Component(
        x.getAs[Int]("idComponent"),
        idVertextList)
    })
    Matching.MergeItems_RecList(recordList, connectedComponentList)
  }
}