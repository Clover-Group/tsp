package ru.itclover.tsp.utils

import java.io.File
import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}

class ArrowOpsTest extends WordSpec with Matchers{

  "ArrowFileReader" should {

    val testPath = Paths.get("flink/src/test/resources/table")

    "parse df" in {

      val result = ArrowOps.readFromFile(testPath.toFile)
      println(s"RESULT: $result")

    }

  }

}
