package demo.model

import demo.util.Normalizer
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, parser}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions

case class ProductionCompany(id: Long, name: String)

object ProductionCompany {

  implicit val decoder: Decoder[ProductionCompany] = deriveDecoder

  def parse(data: String): Option[Seq[ProductionCompany]] = {
    parser.parse(data).right.flatMap(_.as[Seq[ProductionCompany]]) match {
      case Right(v) => Some(v)
      case Left(e) => throw e
    }
  }

  val udfParse: UserDefinedFunction = functions.udf((string: String) => Option(string).flatMap(str => parse(Normalizer.norm(str))))
}
