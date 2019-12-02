package ru.itclover.tsp.utils

import java.io.File
import java.nio.file.{Files => JavaFiles, Paths}
import java.time.LocalDateTime

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.{MessageType, OriginalType}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable
import scala.util.Random

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

    "retrieve schema and reader from bytes" in {

      val testFile = testFiles.head
      val byteData = JavaFiles.readAllBytes(testFile.toPath)

      val schemaAndReader = ParquetOps.retrieveSchemaAndReader(byteData)

      schemaAndReader._1.getClass shouldBe classOf[MessageType]
      schemaAndReader._2.getClass shouldBe classOf[ParquetFileReader]

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

      testFiles.foreach(file => {

        val schemaAndReader = ParquetOps.retrieveSchemaAndReader(file)
        val rowData = ParquetOps.retrieveData(schemaAndReader)

        rowData.head.getArity shouldBe 3

      })

    }

    "retrieve data from bytes" in {

      val testFile = testFiles.head
      val byteData = JavaFiles.readAllBytes(testFile.toPath)

      val schemaAndReader = ParquetOps.retrieveSchemaAndReader(byteData)
      val rowData = ParquetOps.retrieveData(schemaAndReader)

      rowData.head.getArity shouldBe 3

    }

    "write data" in {

      val currentTime = LocalDateTime.now().toString
      val randomInd = Random.nextInt(Integer.MAX_VALUE)

      val tempDir = JavaFiles.createTempDirectory("test")
      val tempPath = Paths.get(tempDir.normalize().toString, s"temp_${randomInd}_($currentTime)", ".temp")
      val tempFile = tempPath.toFile

      val stringSchema =
        s"""message test_data {
           | required int32 a;
           | required binary b;
           |}""".stripMargin

      val data = mutable.ListBuffer(
        mutable.Map(
          "a" -> Tuple2(4, "int"),
          "b" -> Tuple2("test", "java.lang.String")
        ),
        mutable.Map(
          "a" -> Tuple2(5, "int"),
          "b" -> Tuple2("test1", "java.lang.String")
        )
      )

      ParquetOps.writeData((tempFile, stringSchema, data))

      val schemaAndReader = ParquetOps.retrieveSchemaAndReader(tempFile)
      val rowData = ParquetOps.retrieveData(schemaAndReader)

      rowData.head.getArity shouldBe 2

      tempFile.delete()

    }

  }

}
