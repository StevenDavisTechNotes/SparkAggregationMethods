package DedupePerfTest

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.{ mutable }
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset, Row }
import org.apache.spark.sql.functions.{ substring, explode, col, hash, collect_list, struct, concat }
import DedupePerfTest._
import scala.reflect.api.materializeTypeTag

class DedupeUDAF(
  canAssumeNoDupesPerPartition: Boolean)
  extends UserDefinedAggregateFunction {
  def inputSchema =
    RecordMethods.RecordSparseStruct

  def bufferSchema: StructType = StructType(
    StructField("rows", ArrayType(
      RecordMethods.RecordSparseStruct)) :: Nil)

  // This is the output type of your aggregation function.
  def dataType: DataType = ArrayType(
    RecordMethods.RecordSparseStruct)

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer) =
    buffer(0) = Array.empty[Row]

  def addRowToRows(
    lrows: mutable.ArrayBuffer[Record], 
    rrow: Record, nInitialLRows: Int): 
    mutable.ArrayBuffer[Record] = {
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
            Matching.CombineRowList(
              Seq(lrow, rrow))
          val x = lrows(lindex)
          found = true
        }
      }
    }
    if (!found) {
      lrows += rrow
    }
    lrows
  }

  def update(buffer: MutableAggregationBuffer, 
    input: Row) = {
    if (canAssumeNoDupesPerPartition) {
      var result = buffer.getSeq[Row](0) :+ input
      buffer.update(0, result)
    } else {
      val rowInput = new GenericRowWithSchema(
          input.toSeq.toArray, inputSchema)
      val rrow = RecordMethods.rowToRecord(rowInput)
      var lrows = mutable.ArrayBuffer[Record]()
      lrows ++= buffer.getSeq[Row](0)
        .map(RecordMethods.rowToRecord)
      val nInitialLRows = lrows.length
      addRowToRows(lrows, rrow, nInitialLRows)
      buffer.update(0, lrows)
    }
  }

  def merge(
      buffer1: MutableAggregationBuffer, 
      buffer2: Row) = {
    val rrows = buffer2.getSeq[Row](0)
      .map(RecordMethods.rowToRecord)

    var lrows = mutable.ArrayBuffer[Record]()
    lrows ++= buffer1.getSeq[Row](0)
      .map(RecordMethods.rowToRecord)
    val nInitialLRows = lrows.length
    for (rrow <- rrows) {
      addRowToRows(lrows, rrow, nInitialLRows)
    }
    buffer1.update(0, lrows)
  }

  def evaluate(buffer: Row): Any = {
    buffer.getSeq[Row](0)
  }
}

object method_udaf {
  def run(
    generatedDataSet:             
      GeneratedDataSet,
    NumExecutors:                 
      Int,
    canAssumeNoDupesPerPartition: 
      Boolean,
    spark:                        
      SparkSession): 
    (RDD[Record], DataFrame, 
        Dataset[Record]) = {
    import spark.implicits._
    var df = generatedDataSet.df
    df = df
      .withColumn(
        "BlockingKey",
        concat(
          ($"ZipCode"),
          substring($"FirstName", 1, 1),
          substring($"LastName", 1, 1)))
    val cols = df.columns
      .filter(_ != "BlockingKey")
      .filter(_ != "SourceId")
      .map(x => col(x))
    val colNames = df.columns
    val dedupeUDAF = new DedupeUDAF(
        canAssumeNoDupesPerPartition)
    df = df
      .groupBy($"BlockingKey")
      .agg(dedupeUDAF(cols: _*).alias("MergedItems"))
    df = Blocking.UnnestBlocksDataframe(df)
    (null, df, null)
  }
}