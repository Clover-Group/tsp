package ru.itclover.tsp.utils

import java.io.File
import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}

class ArrowOpsTest extends WordSpec with Matchers{

  "ArrowFileReader" should {

    "parse test" in {

      val testFile: File = new File("flink/src/test/resources/test_read.arrow")
      testFile.createNewFile()

      ArrowOps.writeDataToFile(testFile.getAbsolutePath)

      val result = ArrowOps.readFromFile(testFile)
      result.nonEmpty shouldBe true

      testFile.delete()

    }

  }

}
