package demo.model

import demo.util.Normalizer
import io.circe.{Decoder, parser}
import io.circe.generic.semiauto._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions

case class FilmCollection(id: Long, name: String, poster_path: Option[String], backdrop_path: Option[String])

object FilmCollection {

  implicit val decoder: Decoder[FilmCollection] = deriveDecoder

  def parse(data: String): Option[FilmCollection] = {
    parser.parse(data).right.flatMap(_.as[FilmCollection]) match {
      case Right(v) => Some(v)
      case Left(e) => throw e
    }
  }

  val udfParse: UserDefinedFunction = functions.udf((string: String) => Option(string).flatMap(str => parse(Normalizer.norm(str))))
}
