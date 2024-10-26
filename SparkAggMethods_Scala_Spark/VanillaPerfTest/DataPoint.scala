package VanillaPerfTest

import org.apache.spark.sql.{types => DataTypes}
import Common.MathHelper._

final case class DataPoint(
    var id: Long, 
    var grp: Long, 
    var subgrp: Long,
    var A: Long, 
    var B: Long, 
    var C: Double, 
    var D: Double, 
    var E: Double, 
    var F: Double)

final case class AggResult(
    var grp: Long, 
    var subgrp: Long,
    var mean_of_C: Double, 
    var max_of_D: Double, 
    var var_of_E: Double, 
    var var_of_E2: Double)
    
object DataPoint {
  def DataPointSchema: DataTypes.StructType = {
    DataTypes.StructType(
    DataTypes.StructField("id",DataTypes.LongType,false) ::
    DataTypes.StructField("grp",DataTypes.LongType,false) :: 
    DataTypes.StructField("subgrp",DataTypes.LongType,false)::
    DataTypes.StructField("A",DataTypes.LongType,false)::
    DataTypes.StructField("B",DataTypes.LongType,false)::
    DataTypes.StructField("C",DataTypes.DoubleType,false)::
    DataTypes.StructField("D",DataTypes.DoubleType,false)::
    DataTypes.StructField("E",DataTypes.DoubleType,false)::
    DataTypes.StructField("F",DataTypes.DoubleType,false)::
    Nil
    )
  }
  var random = scala.util.Random
  def generateData(numGrp1: Int, numGrp2: Int, repetition: Int) =
    for (i <- 0 until numGrp1 * numGrp2 * repetition)
      yield DataPoint(
            id=i, 
            grp=(i / numGrp2) % numGrp1,
            subgrp=i % numGrp2,
            A=random.nextInt(repetition)+1,
            B=random.nextInt(repetition)+1,
            C=random.nextDouble()*10+1,
            D=random.nextDouble()*10+1,
            E=random.nextGaussian()*10+0,
            F=random.nextGaussian()*10+1)
   def generateCorrectValues(data: IndexedSeq[DataPoint]): Map[(Long,Long),AggResult] = {
      
     val grps = data.groupBy(x=>(x.grp,x.subgrp))
     val inter = grps.map( { case ((grp, subgrp),lst) => {
       val v = variance(lst.map(_.E))
       ((grp, subgrp), AggResult(
           grp, subgrp,
           mean(lst.map(_.C)),
           max(lst.map(_.D).toList).get,
           v, v
           ))}})
     return inter
   }
  def checkData(numGrp1: Int, numGrp2: Int, correct: Map[(Long, Long), AggResult], actual: Iterable[AggResult]): Boolean = {
    val tolerance = 1e-10
    val actMap = actual.groupBy(x=>(x.grp, x.subgrp))
    var maxErrorFound: Double = 0
    for(
        grp <- 0 until numGrp1;
        subgrp <- 0 until numGrp2) {
       val lhs = correct((grp, subgrp))
       val rhs = actMap((grp, subgrp)).head
       maxErrorFound = math.max(maxErrorFound, max(
           math.abs(2*(lhs.mean_of_C - rhs.mean_of_C)/(lhs.mean_of_C + rhs.mean_of_C)) ::
           math.abs(2*(lhs.max_of_D - rhs.max_of_D)/(lhs.max_of_D + rhs.max_of_D)) ::
           math.abs(2*(lhs.var_of_E - rhs.var_of_E)/(lhs.var_of_E + rhs.var_of_E)) ::
           math.abs(2*(lhs.var_of_E2 - rhs.var_of_E2)/(lhs.var_of_E2 + rhs.var_of_E2)) :: 
           Nil).get)
      if(maxErrorFound >= tolerance)
        println(s"!out of tolerance {maxErrorFound} for {grp} {subgrp}")
    }
    return maxErrorFound < tolerance 
  }
}

//var rdd = sc.parallelize(DataPointGenerator.generateData(2,3,5))
//rdd.count()
