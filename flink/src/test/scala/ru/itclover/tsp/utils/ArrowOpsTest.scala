package ru.itclover.tsp.utils

import java.io.File

import org.scalatest.{Matchers, WordSpec}

class ArrowOpsTest extends WordSpec with Matchers{

  "ArrowFileReader" should {

    "work with test file" in {

      val testFile: File = new File("flink/src/test/resources/arrow/aaa2")

      val result = ArrowOps.readFromFile(testFile)
      result.nonEmpty shouldBe true

    }

  }

}
