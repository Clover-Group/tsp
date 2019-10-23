package ru.itclover.tsp.services

import java.io.{File, FileInputStream}
import java.nio.file.Files
import scala.tools.nsc.io.Streamable

import org.apache.arrow.memory.{BaseAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowReader, ArrowStreamReader}
import org.apache.arrow.vector.types.pojo.Schema
import org.scalatest.{Matchers, WordSpec}

class ArrowServiceTest extends WordSpec with Matchers{

  val testFile: File = new File("flink/src/test/resources/arrow/aaa2")

  "ArrowService" should {

    /**"retrieve schema and reader from bytes" in {

      val fileStream = new FileInputStream(testFile)

      val byteArray = Stream.continually(fileStream.read).takeWhile(_ != -1).map(_.toByte).toArray
      val schemaAndReader = ArrowService.retrieveSchemaAndReader(byteArray)

      schemaAndReader._1.getClass shouldBe classOf[Schema]
      schemaAndReader._2.getClass shouldBe classOf[ArrowStreamReader]
      schemaAndReader._3.getClass shouldBe classOf[RootAllocator]

    }*/

    "retrieve schema and reader from file" in {

      val schemaAndReader = ArrowService.retrieveSchemaAndReader(testFile)

      schemaAndReader._1.getClass shouldBe classOf[Schema]
      schemaAndReader._2.getClass shouldBe classOf[ArrowFileReader]
      schemaAndReader._3.getClass shouldBe classOf[RootAllocator]

    }

  }

}
