package test

import atomicflow.*

import java.nio.file.{Files, Path, Paths}
import atomicflow.given
import atomicflow.impl.memory.InMemoryWorkflowRuntime
import ox.*

import cats.syntax.all.*
import scala.annotation.tailrec
import scala.util.Random
import scala.concurrent.duration.*
import scala.util.control.NonFatal
import atomicflow.upickle.CacheableMsgPack.given

object Test3 {
  def readFile(fileName: String): Array[Byte] = s"hello world ${fileName}".getBytes

  def sendFile(bytes: Array[Byte], receiver: String): Unit = ()

  val flow1: Workflow[String, Unit] = Workflow["99a2866c-99c5-49b7-b0f5-ad097a3e3a78"]("read and send files")[String, Unit] { (fileName: String) =>
    val fileBytes: Array[Byte] = Step.cached["533dddc7-d355-43b4-81d8-bd8051808ec5", 0](
      "fileName" -> fileName,
      "random" -> Random.nextInt()
    ) {
      println("reading file")
      readFile(fileName)
    }

    val f: Path = Step.onlyOnce["f4a18269-83a8-4fcf-a62b-3cbb6216ddee", 0](
      "fileBytes" -> fileBytes
    ) {
      println(s"sending file")
      val tmpFile = Files.createTempFile("atomicflow-test", "")
      Files.write(tmpFile, fileBytes)
      tmpFile
    }

    val string: String = Step["9e94a750-59ba-4400-bbde-7cc25a333646", 0] {
      val file2 = f.resolveSibling(f.getFileName.toString + "-")
      Files.readString(file2)
    }

    println(string)
  }

  def runFlow1(in: String)(using WorkflowRuntime): Unit = {
    val instanceId = WorkflowInstanceId.generate
    flow1.create(instanceId, in)

    @tailrec
    def retry: Unit =
      try flow1.recover(instanceId)
      catch {
        case NonFatal(e) =>
          e.printStackTrace()
          sleep(5.seconds)
          retry
      }

    retry
  }

  def main(args: Array[String]): Unit = {
    given WorkflowRuntime = new InMemoryWorkflowRuntime()

    runFlow1("hello world")
  }
}
