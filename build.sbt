name := "Top3Products"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
libraryDependencies += "com.amazon.deequ" % "deequ" % "1.0.2"