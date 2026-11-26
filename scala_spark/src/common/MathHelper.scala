package common

object MathHelper {
  // from https://grokbase.com/t/gg/scala-user/12bptz4gmc/pow-for-int
  def pow(a: Int, b: Int, acc: Int) : Int =
    b match {
      case 0 => acc
      case _ => pow(a, b -1, a * acc)
    }
  def pow(a:Int, b: Int) : Int = { 
    require(b >= 0, "Exponent must be a non-negative integer.")
    pow(a, b, 1)
  }
  // https://stackoverflow.com/questions/39617213/scala-what-is-the-generic-way-to-calculate-standard-deviation
  def mean(a: Seq[Double]): Double = a.sum / a.size
  // Population variance (divide by n), matching the Python strategies and verifier.
  def variance(a: Seq[Double]): Double = {
    val avg = mean(a)
    a.map(x => math.pow((x - avg), 2)).sum / a.size
  }
}