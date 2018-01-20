package demo.model

import demo.util.Normalizer
import io.circe.generic.semiauto._
import io.circe.{Decoder, parser}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions

case class Genre(id: Long, name: String)

object Genre {

  implicit val decoder: Decoder[Genre] = deriveDecoder

  def parse(data: String): Option[Seq[Genre]] = {
    parser.parse(data).right.flatMap(_.as[Seq[Genre]]) match {
      case Right(v) => Some(v)
      case Left(e) => throw e
    }
  }

  val udfParse: UserDefinedFunction = functions.udf((string: String) => Option(string).flatMap(str => parse(Normalizer.norm(str))))
}
