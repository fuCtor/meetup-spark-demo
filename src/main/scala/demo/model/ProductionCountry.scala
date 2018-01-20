package demo.model

import demo.util.Normalizer
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, parser}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions

case class ProductionCountry(iso_3166_1: String, name: String)

object ProductionCountry {

  implicit val decoder: Decoder[ProductionCountry] = deriveDecoder

  def parse(data: String): Option[Seq[ProductionCountry]] = {
    parser.parse(data).right.flatMap(_.as[Seq[ProductionCountry]]) match {
      case Right(v) => Some(v)
      case Left(e) => throw e
    }
  }

  val udfParse: UserDefinedFunction = functions.udf((string: String) => Option(string).flatMap(str => parse(Normalizer.norm(str))))
}