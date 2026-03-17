package atomicflow

import cats.Invariant
import cats.syntax.all.*

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

trait Cacheable[A] {
  def serialize(value: A): IArray[Byte]

  def deserialize(bytes: IArray[Byte]): A
}

object Cacheable {
  def apply[A](using cacheable: Cacheable[A]): Cacheable[A] = cacheable

  given Invariant[Cacheable] = new Invariant[Cacheable] {
    override def imap[A, B](fa: Cacheable[A])(f: A => B)(g: B => A): Cacheable[B] = new Cacheable[B] {
      override def serialize(value: B): IArray[Byte] = fa.serialize(g(value))

      override def deserialize(bytes: IArray[Byte]): B = f(fa.deserialize(bytes))
    }
  }

  object Simple {
    given Cacheable[IArray[Byte]] = new Cacheable[IArray[Byte]] {
      override def serialize(value: IArray[Byte]): IArray[Byte] = value

      override def deserialize(bytes: IArray[Byte]): IArray[Byte] = bytes
    }

    given Cacheable[Array[Byte]] = Cacheable[IArray[Byte]].imap(_.asInstanceOf[Array[Byte]])(_.asInstanceOf[IArray[Byte]])

    given Cacheable[String] = Cacheable[Array[Byte]].imap(new String(_, StandardCharsets.UTF_8))(_.getBytes(StandardCharsets.UTF_8))

    given Cacheable[Long] = Cacheable[String].imap(_.toLong)(_.toString)

    given Cacheable[Int] = Cacheable[String].imap(_.toInt)(_.toString)

    given Cacheable[Double] = Cacheable[String].imap(_.toDouble)(_.toString)

    given Cacheable[Float] = Cacheable[String].imap(_.toFloat)(_.toString)

    given Cacheable[Unit] = new Cacheable[Unit] {
      override def serialize(value: Unit): IArray[Byte] = IArray.empty[Byte]
      override def deserialize(bytes: IArray[Byte]): Unit = ()
    }

    given [A: Cacheable]: Cacheable[Seq[A]] = new Cacheable[Seq[A]] {
      override def serialize(value: Seq[A]): IArray[Byte] = {
        val baos = new ByteArrayOutputStream()
        value.foreach { e =>
          val bytes = Cacheable[A].serialize(e)
          baos.write(bytes.length.toString.getBytes(StandardCharsets.UTF_8))
          baos.write(0x1F)
          baos.write(bytes.asInstanceOf[Array[Byte]])
        }
        baos.toByteArray.asInstanceOf[IArray[Byte]]
      }

      override def deserialize(bytes: IArray[Byte]): Seq[A] = {
        val listBuffer = ListBuffer[A]()
        var remaining = bytes.asInstanceOf[Array[Byte]]
        while (remaining.length > 0) {
          val lengthBytes = remaining.takeWhile(_ != 0x1F)
          val length = new String(lengthBytes, StandardCharsets.UTF_8).toInt
          val dataBytes = remaining.slice(lengthBytes.length + 1, lengthBytes.length + 1 + length)
          listBuffer += Cacheable[A].deserialize(dataBytes.asInstanceOf[IArray[Byte]])
          remaining = remaining.drop(lengthBytes.length + 1 + length)
        }
        listBuffer.toSeq
      }
    }
  }
}
