package ru.itclover.tsp.dsl

import org.scalatest.EitherValues._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.flatspec._

import org.scalatest.matchers.should._

import scala.reflect.ClassTag

// This test explicitly uses Any values.
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class PatternGeneratorTest extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {
  import TestEvents._

  val fieldsClasses = Map(
    "intSensor"     -> ClassTag.Int,
    "longSensor"    -> ClassTag.Long,
    "boolSensor"    -> ClassTag.Boolean,
    "doubleSensor1" -> ClassTag.Double,
    "doubleSensor2" -> ClassTag.Double
  )

  given Conversion[String, String] = _.toString

  val gen = new ASTPatternGenerator[TestEvent, String, Any]

  "Pattern generator" should "build valid patterns" in {

    val patternsList = List(
      "doubleSensor1 > 0 for 30 sec",
      "doubleSensor1 > 0 or longSensor = 0",
      "doubleSensor1 > 0 and intSensor = 0",
      "(doubleSensor1 + intSensor as float64) >= 10",
      "avgOf(doubleSensor1, doubleSensor2) >= 10 for 5 min >= 100 ms",
      "sin(doubleSensor1) >= 0.5 for 5 min andThen intSensor > 42",
      "tan(doubleSensor1) >= 1 for 5 hr andThen avg(doubleSensor2, 3 sec) > 42",
      "count(doubleSensor1, 4 sec) * sum(doubleSensor2, 3 sec) < 9",
      "lag(doubleSensor1, 10 sec) > doubleSensor1"
      // "boolSensor = true andThen boolSensor != false"
    )

    patternsList
      .foreach(pattern =>
        gen
          .build(pattern, 0.0, 1000L, fieldsClasses)
          .right
          .value shouldBe a[(_, PatternMetadata)]
      )
  }

  "Pattern generator" should "not generate invalid patterns" in {
    gen.build("1notAValidName > 0 for 30 sec", 0.0, 1000L, fieldsClasses).left.value shouldBe a[Throwable]
  }

  "Pattern generator" should "not build invalid patterns" in {
    // Assert argument must be boolean
    a[RuntimeException] should be thrownBy gen.generatePattern(Assert(Constant(1.0)))
  }

  "Casts" should "be performed" in {

    val patternsList = List(
      "doubleSensor1 as int32 > 0",
      "doubleSensor1 as int64 > 0",
      "doubleSensor1 as boolean = true"
    )

    patternsList
      .foreach(pattern =>
        gen
          .build(pattern, 0.0, 1000L, fieldsClasses)
          .right
          .value shouldBe a[(_, PatternMetadata)]
      )
  }

  "Syntactic sugar" should "be interpreted" in {
    val sugaredPatternsList = List(
      "doubleSensor1 > 0 for 10 seconds fromStart",
      "doubleSensor1 > 0 for 10 seconds > 3 times fromStart",
      "increasing(doubleSensor1)",
      "decreasing(doubleSensor1)",
      "aligned(doubleSensor1)",
      "doubleSensor1"
    )

    val unsugaredPatternsList = List(
      "wait(10 sec, doubleSensor1 > 0 for 10 sec)",
      "wait(10 sec, doubleSensor1 > 0 for 10 sec > 3 times)",
      "doubleSensor1 > lag(doubleSensor1)",
      "doubleSensor1 < lag(doubleSensor1)",
      "doubleSensor1 = lag(doubleSensor1)",
      "doubleSensor1 as boolean"
    )

    sugaredPatternsList
      .zip(unsugaredPatternsList)
      .foreach((sp, usp) => {
        val sg = gen
          .build(sp, 0.0, 1000L, fieldsClasses)
          .right
          .value
        val usg = gen
          .build(usp, 0.0, 1000L, fieldsClasses)
          .right
          .value
        sg shouldBe a[(_, PatternMetadata)]
        usg shouldBe a[(_, PatternMetadata)]
        sg._1.toString shouldBe usg._1.toString
        // sg._2 shouldBe usg._2
      })
  }

}
