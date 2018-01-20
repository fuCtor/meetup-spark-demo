name := "meetup_demo"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",

  "org.apache.spark" %% "spark-core" % sparkVersion, //% "provided"
  "org.apache.spark" %% "spark-sql" % sparkVersion, //% "provided"
  "org.apache.spark" %% "spark-streaming" % sparkVersion, //% "provided"
  "org.apache.spark" %% "spark-hive" % sparkVersion //% "provided"
)