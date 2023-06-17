package Common

import org.apache.spark.sql.{ DataFrame, Dataset }
import org.apache.spark.sql.types.{ LongType, StructField, StructType }
import org.apache.spark.sql.{ Row, SparkSession }
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object ZipWithIndex {
  def dfZipWithIndex(
    df:      DataFrame,
    offset:  Int       = 1,
    colName: String    = "id",
    inFront: Boolean   = true): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset)))),
      StructType(
        (if (inFront) Array(StructField(colName, LongType, false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName, LongType, false)))))
  }
  def dsZipWithIndex[T, U <: Product : ClassTag : TypeTag](
    df:       Dataset[T],
    addIndex: (T, Long) => U,
    offset:   Int            = 1): Dataset[U] = {
    import df.sqlContext.implicits._
    val rowRDD = df.rdd.zipWithIndex.map(ln => addIndex(ln._1, ln._2))
    df.sqlContext.createDataset(rowRDD)
  }
}