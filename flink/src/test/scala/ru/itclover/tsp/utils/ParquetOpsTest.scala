package ru.itclover.tsp.utils

import java.io.File

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.{MessageType, OriginalType}
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

    "represent types of fields from schema" in {

      testFiles.foreach(file => {

        val schemaAndReader = ParquetOps.retrieveSchemaAndReader(file)
        val schemaTypes = ParquetOps.getSchemaTypes(schemaAndReader._1)

        schemaTypes.keys shouldBe Set("b", "a", "c")
        schemaTypes.values.toList.head._2 shouldBe OriginalType.UTF8


      })

    }

    "retrieve parquet groups" in {

      testFiles.foreach(file => {

        val schemaAndReader = ParquetOps.retrieveSchemaAndReader(file)

        val groups = ParquetOps.getParquetGroups(schemaAndReader._1, schemaAndReader._2)

        groups.size shouldBe 1000000

      })

    }

    "retrieve data" in {

      testFiles.foreach(file =>{

        val schemaAndReader = ParquetOps.retrieveSchemaAndReader(file)
        val rowData = ParquetOps.retrieveData(schemaAndReader)

        rowData.head.getArity shouldBe 1

      })

    }

  }

}
