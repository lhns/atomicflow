package test

import atomicflow.Cacheable
import atomicflow.Cacheable.Simple.given
import cats.syntax.all.*
import munit.FunSuite

class CacheableSuite extends FunSuite {
  case class Amount(cents: Long)

  // v1 stored whole euros as a plain number; v2 stores cents with a format marker
  private val v1: Cacheable[Amount] = Cacheable[Long].imap(euros => Amount(euros * 100))(amount => amount.cents / 100)
  private val v2: Cacheable[Amount] = Cacheable[String].imap { string =>
    require(string.startsWith("v2:"), "not a v2 value")
    Amount(string.stripPrefix("v2:").toLong)
  }(amount => s"v2:${amount.cents}")

  private val evolved = v2.withFallback(v1)

  test("withFallback reads values written in an older format") {
    assertEquals(evolved.deserialize(v1.serialize(Amount(4200))), Amount(4200))
  }

  test("withFallback reads and writes the current format") {
    val bytes = evolved.serialize(Amount(4250))
    assertEquals(new String(bytes.toArray, "UTF-8"), "v2:4250")
    assertEquals(evolved.deserialize(bytes), Amount(4250))
  }

  test("withFallback fails if no format can read the value") {
    intercept[NumberFormatException] {
      evolved.deserialize(IArray.from("garbage".getBytes("UTF-8")))
    }
  }
}
