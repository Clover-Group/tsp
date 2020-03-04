package ru.itclover.tsp.dsl

import cats.Id
import org.scalatest.EitherValues._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.{Fail, IdxValue, Pattern, Patterns, StateMachine, Succ}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

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
      "lag(doubleSensor1, 10 sec) > doubleSensor1",
      "abs(doubleSensor1 - 4.00) < 0.00001 andThen intSensor = 12"
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

  "Run on real data" should "work" in {

    val events = Seq(
      TestEvent(1553545413, 22, 0, boolSensor = false, 6.42, 0.0),
      TestEvent(1553545414, 23, 0, boolSensor = false, 6.42, 0.0),
      TestEvent(1553545415, 23, 0, boolSensor = false, 6.42, 0.0),
      TestEvent(1553545416, 24, 0, boolSensor = false, 6.42, 0.0),
      TestEvent(1553545417, 36, 0, boolSensor = false, 6.0, 0.0),
      TestEvent(1553545418, 36, 0, boolSensor = false, 6.0, 0.0),
      TestEvent(1553545419, 36, 0, boolSensor = false, 6.0, 0.0),
      TestEvent(1553545420, 37, 0, boolSensor = false, 5.88, 0.0),
      TestEvent(1553545421, 37, 0, boolSensor = false, 5.88, 0.0),
      TestEvent(1553545422, 37, 0, boolSensor = false, 6.0, 0.0),
      TestEvent(1553545423, 12, 0, boolSensor = false, 4.01, 0.0),
      TestEvent(1553545424, 12, 0, boolSensor = false, 4.01, 0.0),
      TestEvent(1553545425, 12, 0, boolSensor = false, 4.01, 0.0),
      TestEvent(1553545426, 12, 0, boolSensor = false, 4.0, 0.0),
      TestEvent(1553545427, 11, 0, boolSensor = false, 4.01, 0.0),
      TestEvent(1553545428, 11, 0, boolSensor = false, 4.01, 0.0)
    )

    val pattern = gen
      .build("abs(doubleSensor1 - 4.00) < 0.00001 andThen intSensor = 12", 0.0, 1000L, fieldsClasses)
      .right
      .value
      ._1

    def run[A, S, T](pattern: Pattern[A, S, T], events: Seq[A], groupSize: Int = 1000): ArrayBuffer[IdxValue[T]] = {
      val out = new ArrayBuffer[IdxValue[T]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[T]) => out += x, groupSize)
      out
    }

    val out = run(pattern, events)

    out.size shouldBe 3
    out(0).value.isFail shouldBe true
    out(1).value.isSuccess shouldBe true
    out(2).value.isFail shouldBe true

  }
}
