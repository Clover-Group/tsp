package ru.itclover.tsp.services

import java.io.File

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType
import org.scalatest.{Matchers, WordSpec}

class ParquetServiceTest extends WordSpec with Matchers{

  val testFile: File = new File("flink/src/test/resources/parquet/df_billion.parquet")

  "ParquetService" should {

    "retrieve schema and reader from file" in {

      val schemaAndReader = ParquetService.retrieveSchemaAndReader(testFile)

      schemaAndReader._1.getClass shouldBe classOf[MessageType]
      schemaAndReader._2.getClass shouldBe classOf[ParquetFileReader]

    }

    "read data from file" in {

      val schemaAndReader = ParquetService.retrieveSchemaAndReader(testFile)

      val result = ParquetService.convertData(schemaAndReader)
      result.nonEmpty shouldBe true

    }

  }

}
