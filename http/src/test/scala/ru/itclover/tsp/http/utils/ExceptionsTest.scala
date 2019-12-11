package ru.itclover.tsp.http.utils

import org.scalatest.{FlatSpec, Matchers}

/**
  * Class for testing the exceptions viewer in "core" module
  */
class ExceptionsTest extends FlatSpec with Matchers {

  it should "get string with stacktrace" in {

    val one = 1
    val zero = 0
    val thrownException = the[ArithmeticException] thrownBy one / zero

    val expectedString = "java.lang.ArithmeticException: / by zero"
    val actualString = Exceptions.getStackTrace(thrownException).substring(0, 40)

    actualString shouldBe expectedString

  }

}
