package ru.itclover.tsp.dsl

import org.scalatest.EitherValues._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.ClassTag

// This test explicitly uses Any values.
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class PatternGeneratorTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {
  import TestEvents._

  val fieldsClasses = Map(
    'intSensor     -> ClassTag.Int,
    'longSensor    -> ClassTag.Long,
    'boolSensor    -> ClassTag.Boolean,
    'doubleSensor1 -> ClassTag.Double,
    'doubleSensor2 -> ClassTag.Double
  )

  val gen = new ASTPatternGenerator[TestEvent, Symbol, Any]

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
      //"boolSensor = true andThen boolSensor != false"
    )

    patternsList
      .foreach(
        pattern =>
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
      .foreach(
        pattern =>
          gen
            .build(pattern, 0.0, 1000L, fieldsClasses)
            .right
            .value shouldBe a[(_, PatternMetadata)]
      )
  }
}
