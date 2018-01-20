package demo.util

import org.apache.commons.lang3.StringUtils

object Normalizer {

  private val rgxNone = "\\:\\s?None".r
  private val rgxComa = "'([\\w\\d\\s.\\-\\+\\(\\)\\/\\&]+)'".r
  private val rgxComa2 = "'([\\w\\s]+)\"([\\w\\s]+)\"([\\w\\s]*)'".r

  def norm(string: String): String = {
    val rgxReplacer = Seq(
      rgxComa.replaceAllIn(_: String, (m) => "\"" + m.group(1) + "\""),
      rgxComa2.replaceAllIn(_: String, (m) => "\"" + m.group(1) + "\\\\\"" + m.group(2) + "\\\\\"" + m.group(3) + "\""),
      rgxNone.replaceAllIn(_: String, ": null")
    ).fold((s: String) => s)((a, b) => a.andThen(b))

    rgxReplacer(StringUtils.stripAccents(string))
      .replace(": '", ": \"")
      .replace("', \"", "\", \"")
      .replace("'}", "\"}")
  }
}