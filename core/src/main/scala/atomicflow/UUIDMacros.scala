package atomicflow

import scala.quoted.*

object UUIDMacros {
  private val uuidRegex = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}".r

  inline def validateUUID(inline s: String): String = ${ validateUUIDImpl('s) }

  private def validateUUIDImpl(s: Expr[String])(using Quotes): Expr[String] = {
    import quotes.reflect.*
    s.value match {
      case Some(str) =>
        if (uuidRegex.matches(str)) s
        else report.errorAndAbort(s"Invalid UUID format: \"$str\". Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
      case None =>
        report.errorAndAbort("UUID must be a string literal")
    }
  }
}
