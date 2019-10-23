package ru.itclover.tsp.services

import java.io.File

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.types.pojo.Schema
import org.scalatest.{Matchers, WordSpec}

class ArrowServiceTest extends WordSpec with Matchers{
  
  val testFile: File = new File("flink/src/test/resources/arrow/df_billion")

  "ArrowService" should {

    "retrieve schema and reader from file" in {

      val schemaAndReader = ArrowService.retrieveSchemaAndReader(testFile)

      schemaAndReader._1.getClass shouldBe classOf[Schema]
      schemaAndReader._2.getClass shouldBe classOf[ArrowFileReader]
      schemaAndReader._3.getClass shouldBe classOf[RootAllocator]

    }

    "read data from file" in {

      val schemaAndReader = ArrowService.retrieveSchemaAndReader(testFile)

      val result = ArrowService.convertData(schemaAndReader)

      result.nonEmpty shouldBe true

    }

  }

}
