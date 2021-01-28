package ru.itclover.tsp.utils

import java.io.File
import java.nio.file.{Files => JavaFiles}

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowStreamReader}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.services.FileService

import scala.collection.JavaConverters._
import scala.collection.mutable

class ArrowOpsTest extends WordSpec with Matchers {

  val testFiles: List[File] = new File(
    "flink/src/test/resources/arrow"
  ).listFiles()
    .filter(_.isFile)
    .toList

  "ArrowOps" should {

    "retrieve schema and reader from file" in {

      testFiles.foreach(file => {

        val schemaAndReader = ArrowOps.retrieveSchemaAndReader(file, Integer.MAX_VALUE)

        schemaAndReader._1.getClass shouldBe classOf[Schema]
        schemaAndReader._2.getClass shouldBe classOf[ArrowFileReader]
        schemaAndReader._3.getClass shouldBe classOf[RootAllocator]

      })

    }

//    "retrieve data" in {
//
//      testFiles.foreach(file => {
//
//        val schemaAndReader = ArrowOps.retrieveSchemaAndReader(file, Integer.MAX_VALUE)
//        val rowData = ArrowOps.retrieveData(schemaAndReader)
//
//        rowData.head.getArity shouldBe 3
//
//      })
//
//    }
//
//    "write data" in {
//
//      val tempPath = FileService.createTemporaryFile()
//      val tempFile = tempPath.toFile
//
//      val schemaFields = List(
//        new Field(
//          "a",
//          false,
//          new ArrowType.Int(32, true),
//          null
//        ),
//        new Field(
//          "b",
//          false,
//          new ArrowType.Utf8,
//          null
//        )
//      )
//
//      val schema = new Schema(schemaFields.asJava)
//      val allocator = new RootAllocator(1000000)
//
//      val data = mutable.ListBuffer(
//        mutable.Map(
//          "a" -> 4,
//          "b" -> "test"
//        ),
//        mutable.Map(
//          "a" -> 5,
//          "b" -> "test1"
//        )
//      )
//
//      ArrowOps.writeData((tempFile, schema, data, allocator))
//
//      val schemaAndReader = ArrowOps.retrieveSchemaAndReader(tempFile, Integer.MAX_VALUE)
//      ArrowOps.retrieveData(schemaAndReader)
//
//      tempFile.delete()
//
//    }
//
  }

}
