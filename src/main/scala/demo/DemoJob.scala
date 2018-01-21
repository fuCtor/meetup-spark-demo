package demo

import demo.model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object DemoJob extends App {
  implicit val appContext = Context("demo_app")

  val rating = readCSV("data/ratings_small.csv")
  val moviesMetadata = readCSV("data/movies_metadata.csv").repartition(1)

  val df = moviesMetadata
    .withColumn("id", col("id").cast(IntegerType)).where(col("id") > 0)

  val expandedDf = Seq(
    toStruct(_: DataFrame, "belongs_to_collection", FilmCollection.udfParse),
    toStruct(_: DataFrame, "genres", Genre.udfParse),
    toStruct(_: DataFrame, "production_companies", ProductionCompany.udfParse),
    toStruct(_: DataFrame, "production_countries", ProductionCountry.udfParse),
    toStruct(_: DataFrame, "spoken_languages", Language.udfParse)
  ).fold((x: DataFrame) => x)((a, b) => a.andThen(b))(df).cache()

  val movieGenres = explodeSeqStruct(expandedDf, "genres")
  val movieLanguages = explodeSeqStruct(expandedDf, "spoken_languages")
  val movieCompanies = explodeSeqStruct(expandedDf, "production_companies")
  val movieCountries = explodeSeqStruct(expandedDf, "production_countries")
  val movieCollection = expandedDf.where(col("belongs_to_collection").isNotNull).select(col("id").as("movie_id"), col("belongs_to_collection.*"))

  val genreByCountry = movieGenres.withColumnRenamed("name", "genre").join(movieCountries.withColumnRenamed("name", "country"), movieCountries("movie_id") === movieGenres("movie_id"))

  genreByCountry.select("genre", "country").distinct().orderBy("country") show 10

  appContext.stop()

  def readCSV(path: String)(implicit context: Context): DataFrame =
    context.spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv(path)

  def toStruct(df: DataFrame, name: String, extractor: UserDefinedFunction): DataFrame =
    df.withColumn(name, extractor(col(name)))

  def explodeSeqStruct(df: DataFrame, name: String): DataFrame =
    df.select(col("id").as("movie_id"), explode(col(name)).as("tmp")).where(col("tmp").isNotNull)
      .select(col("movie_id"), col("tmp.*"))

}
