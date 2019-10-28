package ru.itclover.tsp.utils

import java.io.File

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType
import org.scalatest.{Matchers, WordSpec}

class ParquetOpsTest extends WordSpec with Matchers {

  val testFiles: List[File] = new File(
    "flink/src/test/resources/parquet"
  ).listFiles()
    .filter(_.isFile)
    .toList

  "ParquetOps" should {

    "retrieve schema and reader from file" in {

      testFiles.foreach(file => {

        val schemaAndReader = ParquetOps.retrieveSchemaAndReader(file)

        schemaAndReader._1.getClass shouldBe classOf[MessageType]
        schemaAndReader._2.getClass shouldBe classOf[ParquetFileReader]

      })

    }

  }

}
