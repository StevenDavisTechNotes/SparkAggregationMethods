package challenges.vanilla

import org.apache.spark.sql.{types => DataTypes}
import common.MathHelper._

final case class DataPoint(
    id: Long, 
    grp: Long, 
    subgrp: Long,
    A: Long, 
    B: Long, 
    C: Double, 
    D: Double, 
    E: Double, 
    F: Double)

final case class AggResult(
    grp: Long, 
    subgrp: Long,
    mean_of_C: Double, 
    max_of_D: Double, 
    var_of_E: Double, 
    var_of_E2: Double)
    
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
  private val random = new scala.util.Random()
  def generateData(numGrp1: Int, numGrp2: Int, repetition: Int): IndexedSeq[DataPoint] =
    for (i <- 0 until numGrp1 * numGrp2 * repetition)
      yield DataPoint(
        id = i,
        grp = (i / numGrp2) % numGrp1,
        subgrp = i % numGrp2,
        A = random.nextInt(repetition) + 1,
        B = random.nextInt(repetition) + 1,
        C = random.nextDouble() * 10 + 1,
        D = random.nextDouble() * 10 + 1,
        E = random.nextGaussian() * 10 + 0,
        F = random.nextGaussian() * 10 + 1)

  def generateCorrectValues(data: IndexedSeq[DataPoint]): Map[(Long, Long), AggResult] =
    data.groupBy(x => (x.grp, x.subgrp)).map {
      case ((grp, subgrp), lst) =>
        val v = variance(lst.map(_.E))
        (grp, subgrp) -> AggResult(grp, subgrp, mean(lst.map(_.C)), lst.map(_.D).max, v, v)
    }

  private def relativeError(lhs: Double, rhs: Double): Double =
    math.abs(2 * (lhs - rhs) / (lhs + rhs))

  def checkData(
      numGrp1: Int,
      numGrp2: Int,
      correct: Map[(Long, Long), AggResult],
      actual: Iterable[AggResult]): Boolean = {
    val tolerance = 1e-10
    val actMap = actual.groupBy(x => (x.grp, x.subgrp))
    val errors = for (
      grp <- 0 until numGrp1;
      subgrp <- 0 until numGrp2
    ) yield {
      val lhs = correct((grp, subgrp))
      val rhs = actMap((grp, subgrp)).head
      val error = Seq(
        relativeError(lhs.mean_of_C, rhs.mean_of_C),
        relativeError(lhs.max_of_D, rhs.max_of_D),
        relativeError(lhs.var_of_E, rhs.var_of_E),
        relativeError(lhs.var_of_E2, rhs.var_of_E2)).max
      if (error >= tolerance)
        LogHolder.log.error(s"Out of tolerance $error for $grp $subgrp")
      error
    }
    errors.max < tolerance
  }
}
