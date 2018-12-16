package DedupePerfTest

import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset, Row }
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{ Encoder, Encoders }
import scala.collection.mutable.ListBuffer

object method_aggregator {
  class Dedupe(canAssumeNoDupesPerPartition: Boolean)
    extends Aggregator[Record, ListBuffer[Record], Seq[Record]]
    with Serializable {

    def addRowToRows(lrows: ListBuffer[Record], rrow: Record, nInitialLRows: Int): ListBuffer[Record] = {
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

    def zero = new ListBuffer[Record]
    def reduce(lrows: ListBuffer[Record], rrow: Record): ListBuffer[Record] = {
      if (canAssumeNoDupesPerPartition) {
        lrows :+ rrow
      } else {
        addRowToRows(lrows, rrow, lrows.length)
      }
    }
    def merge(lrows: ListBuffer[Record], rrows: ListBuffer[Record]) = {
      val nInitialLRows = lrows.length
      for (rrow <- rrows) {
        addRowToRows(lrows, rrow, nInitialLRows)
      }
      lrows
    }
    def finish(acc: ListBuffer[Record]) = acc

    def bufferEncoder: Encoder[ListBuffer[Record]] = ExpressionEncoder()
    def outputEncoder: Encoder[Seq[Record]] = ExpressionEncoder()
  }

  def run(
    generatedDataSet:             GeneratedDataSet,
    NumExecutors:                 Int,
    canAssumeNoDupesPerPartition: Boolean,
    spark:                        SparkSession): (RDD[Record], DataFrame, Dataset[Record]) = {
    import spark.implicits._

    val ds = generatedDataSet.ds
      .groupByKey(Blocking.BlockingFunction _)
      .agg(new Dedupe(canAssumeNoDupesPerPartition).toColumn)
      .flatMap(x => x._2)
    (null, null, ds)
  }
}
