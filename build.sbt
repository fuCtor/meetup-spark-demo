import sbt.Keys.libraryDependencies

name := "meetup_demo"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.0"
val circeVersion = "0.9.0-M1"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",

  "io.circe" %% "circe-generic" % circeVersion excludeAll ExclusionRule(organization = "ch.qos.logback"),
  "io.circe" %% "circe-parser" % circeVersion,

  "org.apache.spark" %% "spark-core" % sparkVersion, //% "provided"
  "org.apache.spark" %% "spark-sql" % sparkVersion, //% "provided"
  "org.apache.spark" %% "spark-streaming" % sparkVersion, //% "provided"
  "org.apache.spark" %% "spark-hive" % sparkVersion, //% "provided"

  "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4" exclude("com.typesafe.scala-logging", "scala-logging-slf4j_2.11")
)