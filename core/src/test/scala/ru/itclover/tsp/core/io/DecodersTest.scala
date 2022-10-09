package ru.itclover.tsp.core.io

import org.scalatest.wordspec._

import org.scalatest.matchers.should._


/**
  * Test class for decoders
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class DecodersTest extends AnyWordSpec with Matchers {

  "any decoders" should {

    val doubleDecoder = AnyDecodersInstances.decodeToDouble
    val intDecoder = AnyDecodersInstances.decodeToInt
    val longDecoder = AnyDecodersInstances.decodeToLong
    val booleanDecoder = AnyDecodersInstances.decodeToBoolean

    val expectedDoubleResult = 5.0
    val expectedIntResult = 5
    val expectedLongResult = 5L

    "decode double from double" in {
      val actualResult = doubleDecoder.apply(5.0)

      actualResult shouldBe expectedDoubleResult
    }

    "decode double from number" in {
      val actualResult = doubleDecoder.apply(5)

      actualResult shouldBe expectedDoubleResult
    }

    "decode double from string" in {
      val actualResult = doubleDecoder.apply("5")

      actualResult shouldBe expectedDoubleResult
    }

//    "raise error from bad double string" in {
//      val testString = "test"
//      val thrownException = the[RuntimeException] thrownBy doubleDecoder.apply(testString)
//
//      assert(thrownException.getMessage contains s"Cannot parse String ($testString) to Double")
//
//    }

    "decode int from number" in {
      val actualResult = intDecoder.apply(5.0)

      actualResult shouldBe expectedIntResult
    }

    "decode int from string" in {
      val actualResult = intDecoder.apply("5")

      actualResult shouldBe expectedIntResult
    }

    "raise error from bad int string" in {

      val testString = "test"
      val thrownException = the[RuntimeException] thrownBy intDecoder.apply(testString)

      assert(thrownException.getMessage contains s"Cannot parse String ($testString) to Int")

    }

    "decode long from number" in {
      val actualResult = longDecoder.apply(5)

      actualResult shouldBe expectedLongResult
    }

    "decode long from string" in {
      val actualResult = longDecoder.apply("5")

      actualResult shouldBe expectedLongResult
    }

    "raise error from bad long string" in {

      val testString = "test"
      val thrownException = the[RuntimeException] thrownBy longDecoder.apply(testString)

      assert(thrownException.getMessage contains s"Cannot parse String ($testString) to Long")

    }

    "decode boolean from numbers" in {

      val input = List(1, 1L, 1.0)

      input
        .foreach(item => assert(booleanDecoder.apply(item)))

    }

    "decode boolean from strings" in {

      val input = List("true", "on", "yes")

      input
        .foreach(item => assert(booleanDecoder.apply(item)))

    }

    "raise error from bad boolean string" in {

      val testString = "test"
      val thrownException = the[RuntimeException] thrownBy booleanDecoder.apply(testString)

      assert(thrownException.getMessage contains s"Cannot parse '$testString' to Boolean")

    }

  }

}
