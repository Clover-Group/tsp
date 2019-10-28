package ru.itclover.tsp.utils

import java.io.File

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.types.pojo.Schema
import org.scalatest.{Matchers, WordSpec}

class ArrowOpsTest extends WordSpec with Matchers{

  val testFiles: List[File] = new File(
    "flink/src/test/resources/arrow"
  ).listFiles()
   .filter(_.isFile)
   .toList

  "ArrowOps" should{

    "retrieve schema and reader from file" in {

      testFiles.foreach(file => {

        val schemaAndReader = ArrowOps.retrieveSchemaAndReader(file, Integer.MAX_VALUE)

        schemaAndReader._1.getClass shouldBe classOf[Schema]
        schemaAndReader._2.getClass shouldBe classOf[ArrowFileReader]
        schemaAndReader._3.getClass shouldBe classOf[RootAllocator]

      })

    }

    "retrieve data" in {

       testFiles.foreach(file => {

         val schemaAndReader = ArrowOps.retrieveSchemaAndReader(file, Integer.MAX_VALUE)
         val rowData = ArrowOps.retrieveData(schemaAndReader)

         rowData.head.getArity shouldBe 3

       })

    }

  }

}
