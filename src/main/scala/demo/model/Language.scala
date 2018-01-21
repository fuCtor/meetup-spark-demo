package demo.model

import demo.util.Normalizer
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, parser}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions

case class Language(iso_639_1: String, name: String)

object Language {

  implicit val decoder: Decoder[Language] = deriveDecoder

  def parse(data: String): Option[Seq[Language]] = {
    parser.parse(data).right.flatMap(_.as[Seq[Language]]) match {
      case Right(v) => Some(v)
      case Left(e) => None
    }
  }

  val udfParse: UserDefinedFunction = functions.udf((string: String) => Option(string).flatMap(str => parse(Normalizer.norm(str))))
}
