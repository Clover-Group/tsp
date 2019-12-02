package ru.itclover.tsp.services

import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class FileServiceTest extends WordSpec with Matchers {

  "FileService" should {

    "convert bytes to file" in {

      val randomBytes = Array.fill(20)((Random.nextInt(256) - 128).toByte)
      val resultFile = FileService.convertBytes(randomBytes)

      assert(resultFile.exists())
      assert(resultFile.getName.contains("temp"))
      assert(resultFile.length() > 0)

      resultFile.delete()

    }

  }

}
