name := "interest"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib-local" % "2.4.0",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.0",
  "org.apache.derby" % "derby" % "10.4.1.3" % Test,
  "org.scalatest"%%"scalatest"%"3.2.0-SNAP9",
  "org.scalanlp" %% "breeze" % "0.13.2",
  "org.apache.spark" %% "spark-mllib" % "2.4.0" % "runtime",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-graphx
  "org.apache.spark" %% "spark-graphx" % "2.4.0",
  "mysql" % "mysql-connector-java" % "5.1.47"

)