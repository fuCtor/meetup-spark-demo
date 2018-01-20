package demo

import org.apache.spark.sql.DataFrame

object DemoJob extends App {
  implicit val appContext = Context("demo_app")

  val rating = readCSV("data/ratings_small.csv")
  val moviesMetadata = readCSV("data/movies_metadata.csv")

  rating.show(10)
  moviesMetadata.show(10)

  moviesMetadata.printSchema()

  appContext.stop()

  def readCSV(path: String)(implicit context: Context): DataFrame =
    context.spark.read.option("header", "true").csv(path)
}
