package demo

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class Context(appName: String) extends LazyLogging {
  val sparkConfig: SparkConf = {
    val conf = new SparkConf().setAppName(appName)
    Option(System.getenv("SPARK_MASTER_URL")) match { // https://github.com/big-data-europe/docker-spark
      case Some(_) => conf
      case None => conf.setMaster("local[*]")
    }
  }

  val sc = new SparkContext(sparkConfig)

  lazy val spark: SparkSession = {
    val session = SparkSession.builder
      .appName(appName)
      .config("spark.sql.session.timeZone", "UTC")
      .enableHiveSupport()

    session.getOrCreate()
  }

  lazy val sql = spark.sqlContext

  def stop(): Unit = {
    logger.info("Stop Spark")
    sc.stop()
  }
}

object Context {
  def apply(appName: String): Context = new Context(appName)
}
