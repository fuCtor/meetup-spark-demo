package demo

import demo.model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object DemoJob extends App {
  implicit val appContext = Context("demo_app")

  val rating = readCSV("data/ratings_small.csv")
  val moviesMetadata = readCSV("data/movies_metadata.csv")

  val df = moviesMetadata
    .withColumn("id", col("id").cast(IntegerType)).where(col("id") > 0)

  val expandedDf = Seq(
    toStruct(_: DataFrame, "belongs_to_collection", FilmCollection.udfParse),
    toStruct(_: DataFrame, "genres", Genre.udfParse),
    toStruct(_: DataFrame, "production_companies", ProductionCompany.udfParse),
    toStruct(_: DataFrame, "production_countries", ProductionCountry.udfParse),
    toStruct(_: DataFrame, "spoken_languages", Language.udfParse)
  ).fold((x: DataFrame) => x)((a, b) => a.andThen(b))(df).cache()

  val movieGenres = expandedDf
    .select(col("id").as("movie_id"), explode(col("genres")).as("genre"))
    .select(col("movie_id"), col("genre.*"))

  movieGenres.show(10)

  appContext.stop()

  def readCSV(path: String)(implicit context: Context): DataFrame =
    context.spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv(path)

  def toStruct(df: DataFrame, name: String, extractor: UserDefinedFunction): DataFrame =
    df.withColumn(name, extractor(col(name)))

}
