package ru.itclover.tsp.utils

import java.io.File
import org.scalatest.{Matchers, WordSpec}

class ParquetOpsTest extends WordSpec with Matchers{

  "ParquetFileReader" should {

    "work with test file" in {

      val testFile: File = new File("flink/src/test/resources/parquet/test_read.pq")

      val testList = List(TempSchema("test", 11), TempSchema("test1", 12))

      ParquetOps.writeToFile(testFile, testList)

      val readResult: List[TempSchema] = ParquetOps.readFromFile(testFile)

      readResult shouldBe testList

      testFile.delete()


    }

  }

}
