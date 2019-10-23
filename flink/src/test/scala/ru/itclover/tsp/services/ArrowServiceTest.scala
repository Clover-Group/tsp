package ru.itclover.tsp.services

import java.io.File
import java.nio.file.Files

import org.apache.arrow.memory.BaseAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import org.scalatest.{Matchers, WordSpec}

class ArrowServiceTest extends WordSpec with Matchers{

  val testFile: File = new File("flink/src/test/resources/arrow/aaa2")

  "ArrowService" should {

    "retrieve schema and reader from bytes" in {

      val byteArray = Files.readAllBytes(testFile.toPath)
      val schemaAndReader = ArrowService.retrieveSchemaAndReader(byteArray)

      schemaAndReader._1.getClass shouldBe classOf[Schema]
      schemaAndReader._2.getClass shouldBe classOf[ArrowReader]
      schemaAndReader._3.getClass shouldBe classOf[BaseAllocator]

    }

    "retrieve schema and reader from file" in {

      val schemaAndReader = ArrowService.retrieveSchemaAndReader(testFile)

      schemaAndReader._1.getClass shouldBe classOf[Schema]
      schemaAndReader._2.getClass shouldBe classOf[ArrowReader]
      schemaAndReader._3.getClass shouldBe classOf[BaseAllocator]

    }

  }

}
