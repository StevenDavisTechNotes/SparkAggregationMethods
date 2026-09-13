name := "SparkAggregationMethods"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.18"

Compile / scalaSource := baseDirectory.value / "src"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.9" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.9" % "provided",
  "com.chuusai" %% "shapeless" % "2.3.10"
)
