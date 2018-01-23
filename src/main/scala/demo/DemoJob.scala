package demo

import demo.model._
import demo.util.Neo4J
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object DemoJob extends App {
  implicit val appContext = Context("demo_app")

  val rating = readCSV("data/ratings_small.csv")
    .withColumnRenamed("userId", "user_id")
    .withColumnRenamed("movieId", "movie_id").withColumn("movie_id", col("movie_id").cast(LongType))
    .cache()
  val moviesMetadata = readCSV("data/movies_metadata.csv")

  val df = moviesMetadata
    .withColumn("id", col("id").cast(LongType))
    .where(col("id") > 0).withColumnRenamed("id", "movie_id")

  val expandedDf = Seq(
    toStruct(_: DataFrame, "belongs_to_collection", FilmCollection.udfParse),
    toStruct(_: DataFrame, "genres", Genre.udfParse),
    toStruct(_: DataFrame, "production_companies", ProductionCompany.udfParse),
    toStruct(_: DataFrame, "production_countries", ProductionCountry.udfParse),
    toStruct(_: DataFrame, "spoken_languages", Language.udfParse)
  ).fold((x: DataFrame) => x)((a, b) => a.andThen(b))(df).cache()

  val movieGenres = explodeSeqStruct(expandedDf, "genres").withColumnRenamed("id", "genre_id")
  val movieLanguages = explodeSeqStruct(expandedDf, "spoken_languages")
  val movieCompanies = explodeSeqStruct(expandedDf, "production_companies").withColumnRenamed("id", "company_id")
  val movieCountries = explodeSeqStruct(expandedDf, "production_countries")
  val movieCollections = expandedDf.where(col("belongs_to_collection").isNotNull)
    .select(col("movie_id"), col("belongs_to_collection.*")).withColumnRenamed("id", "collection_id")

  val movies = expandedDf.select("movie_id", "title", "status", "budget", "vote_average", "vote_count").dropDuplicates("movie_id")

  val genres = movieGenres.select("genre_id", "name").distinct()
  val companies = movieCompanies.select("company_id", "name").distinct()
  val countries = movieCountries.select("iso_3166_1", "name").distinct()
  val languages = movieLanguages.select("iso_639_1", "name").distinct()
  val collections = movieCollections.select("collection_id", "name").distinct()
  val users = rating.select("user_id").distinct()

  Neo4J.saveNodes(movies, "MOVIE", "movie_id" )
  Neo4J.saveNodes(genres, "GENRE", "genre_id")
  Neo4J.saveNodes(companies, "COMPANY", "company_id")
  Neo4J.saveNodes(countries, "COUNTRY", "iso_3166_1")
  Neo4J.saveNodes(languages, "LANGUAGE", "iso_639_1")
  Neo4J.saveNodes(collections, "COLLECTION", "collection_id")
  Neo4J.saveNodes(users, "USER", "user_id")

  Neo4J.saveEdges(movieGenres, "MOVIE" -> "movie_id", "TAGGED" -> Seq.empty, "GENRE" -> "genre_id")
  Neo4J.saveEdges(movieCompanies, "MOVIE" -> "movie_id", "PRODUCTION_BY" -> Seq.empty, "COMPANY" -> "company_id")
  Neo4J.saveEdges(movieCountries, "MOVIE" -> "movie_id", "PRODUCTION_IN" -> Seq.empty, "COUNTRY" -> "iso_3166_1")
  Neo4J.saveEdges(movieLanguages, "MOVIE" -> "movie_id", "SPEAK" -> Seq.empty, "LANGUAGE" -> "iso_639_1")
  Neo4J.saveEdges(movieCollections, "MOVIE" -> "movie_id", "IN" -> Seq.empty, "COLLECTION" -> "collection_id")
  Neo4J.saveEdges(rating, "USER" -> "user_id", "RATE" -> Seq("rating"), "MOVIE" -> "movie_id")

  appContext.stop()

  def readCSV(path: String)(implicit context: Context): DataFrame =
    context.spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv(path)

  def toStruct(df: DataFrame, name: String, extractor: UserDefinedFunction): DataFrame =
    df.withColumn(name, extractor(col(name)))

  def explodeSeqStruct(df: DataFrame, name: String): DataFrame =
    df.select(col("movie_id"), explode(col(name)).as("tmp")).where(col("tmp").isNotNull)
      .select(col("movie_id"), col("tmp.*"))

}
