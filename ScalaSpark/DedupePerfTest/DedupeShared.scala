package DedupePerfTest

import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.{ SparkSession, Row, DataFrame, Dataset, Encoder }
import org.apache.spark.sql.types._
import org.apache.log4j.Logger

object DedupeShared {
  def MinNotNull[X >: Null <: Comparable[X]](lst:Seq[X]): X = {
    if(lst.isEmpty) {
      return null
    }
    lst.reduce ( (x,y) => {
      if(x != null) {
        if(y != null) {
          if(x.compareTo(y)>0) x else y
        } else {
          x
        }
      } else {
        y
      }
    })
  }
  
  def FirstNotNull[X >: Null](lst: Seq[X]): X = {
    for(x <- lst) {
      if(x != null) {
        return x
      }
    }
    return null
  }

}