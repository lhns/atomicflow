package atomicflow.upickle

import atomicflow.Cacheable
import cats.syntax.all.*
import upickle.default.*

import java.nio.file.{Path, Paths}
import java.time.*

object CacheableMsgPack {
  given [A: {Writer, Reader}]: Cacheable[A] = new Cacheable[A] {
    override def serialize(value: A): IArray[Byte] =
      writeBinary(value).asInstanceOf[IArray[Byte]]

    override def deserialize(bytes: IArray[Byte]): A =
      readBinary[A](bytes.asInstanceOf[Array[Byte]])
  }

  given Cacheable[Path] = Cacheable[String].imap(Paths.get(_))(_.toString)

  given Cacheable[Instant] = Cacheable[String].imap(Instant.parse)(_.toString)

  given Cacheable[LocalDate] = Cacheable[String].imap(LocalDate.parse)(_.toString)

  given Cacheable[LocalDateTime] = Cacheable[String].imap(LocalDateTime.parse)(_.toString)

  given Cacheable[OffsetDateTime] = Cacheable[String].imap(OffsetDateTime.parse)(_.toString)

  given Cacheable[ZonedDateTime] = Cacheable[String].imap(s => ZonedDateTime.parse(s))(_.toString)
}
