package DedupePerfTest

import scala.collection.mutable.{ HashMap, ListBuffer }
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset, Row }
import org.apache.spark.{ HashPartitioner }

object method_rdd_mappart {
  def run(
    generatedDataSet: GeneratedDataSet,
    NumExecutors:     Int,
    canAssumeNoDupesPerPartition: Boolean,
    spark:            SparkSession): (RDD[Record], DataFrame, Dataset[Record]) = {
    val rdd = generatedDataSet.rdd
      .keyBy(Blocking.BlockingFunction)
      .partitionBy(new HashPartitioner(NumExecutors))
      .mapPartitions(core_mappart)
    (rdd, null, null)
  }
  def AddRowToRowList(rows: ListBuffer[Record], jrow: Record) = {
    var found = false
    for ((irow, index) <- rows.zipWithIndex) {
      if (!found) {
        if (Matching.IsMatch(
          irow.FirstName, jrow.FirstName,
          irow.LastName, jrow.LastName,
          irow.ZipCode, jrow.ZipCode,
          irow.SecretKey, jrow.SecretKey)) {
          rows(index) = Matching.CombineRowList(Seq(irow, jrow))
          found = true
        }
      }
    }
    if (!found) {
      rows.append(jrow)
    }
  }
  def core_mappart(iterator: Iterator[Tuple2[String, Record]]): Iterator[Record] = {
    var acc = new HashMap[String, ListBuffer[Record]]
    for ((key, rec) <- iterator) {
      if (!acc.contains(key)) {
        val rows = new ListBuffer[Record]
        rows += rec
        acc(key) = rows
      } else {
        AddRowToRowList(acc(key), rec)
      }
    }
    acc.valuesIterator.flatten
  }
}