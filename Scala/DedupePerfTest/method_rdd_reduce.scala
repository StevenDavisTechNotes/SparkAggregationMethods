package DedupePerfTest

import scala.collection.{ mutable }
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset, Row }

object method_rdd_reduce {
  def addRowToRows(
      lrows: mutable.ListBuffer[Record], 
      rrow: Record, nInitialLRows: Int): 
      mutable.ListBuffer[Record] = {
    var found = false
    for (lindex <- 0 until nInitialLRows) {
      if (!found) {
        var lrow = lrows(lindex)
        if (Matching.IsMatch(
          lrow.FirstName, rrow.FirstName,
          lrow.LastName, rrow.LastName,
          lrow.ZipCode, rrow.ZipCode,
          lrow.SecretKey, rrow.SecretKey)) {
          lrows(lindex) = 
            Matching.CombineRowList(Seq(lrow, rrow))
          found = true
        }
      }
    }
    if (!found) {
      lrows += rrow
    }
    lrows
  }
  def seqOpDisjoint(
      lrows: mutable.ListBuffer[Record], 
      rrow: Record): 
      mutable.ListBuffer[Record] = {
    lrows :+ rrow
  }
  def seqOpMixed(
      lrows: mutable.ListBuffer[Record], 
      rrow: Record): 
      mutable.ListBuffer[Record] = {
    addRowToRows(lrows, rrow, lrows.length)
  }
  def combOp(
      lrows: mutable.ListBuffer[Record], 
      rrows: mutable.ListBuffer[Record]): 
      mutable.ListBuffer[Record] = {
    val nInitialLRows = lrows.length
    for (rrow <- rrows) {
      addRowToRows(lrows, rrow, nInitialLRows)
    }
    lrows
  }
  def run(
    generatedDataSet:             GeneratedDataSet,
    NumExecutors:                 Int,
    canAssumeNoDupesPerPartition: Boolean,
    spark:                        SparkSession): 
    (RDD[Record], DataFrame, Dataset[Record]) = {
    val seqOp = if (canAssumeNoDupesPerPartition) 
      seqOpDisjoint _ else seqOpMixed _
    val rdd = generatedDataSet.rdd
      .keyBy(Blocking.BlockingFunction)
      .aggregateByKey(mutable.ListBuffer.empty[Record])(
        seqOp,
        combOp _)
      .flatMap(_._2)
    (rdd, null, null)
  }
}