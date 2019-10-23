package ru.itclover.tsp.utils

import java.io.File
import org.scalatest.{Matchers, WordSpec}

class ParquetOpsTest extends WordSpec with Matchers{

  "ParquetFileReader" should {

    "work with test file" in {

      val testFile: File = new File("flink/src/test/resources/parquet/df_billion_brotli.parquet")

      val readResult: List[TempSchema] = ParquetOps.readFromFile(testFile)

      readResult.nonEmpty shouldBe true


    }

  }

}
