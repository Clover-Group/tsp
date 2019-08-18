package ru.itclover.tsp.dsl.v2
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import org.scalatest.EitherValues._
import ru.itclover.tsp.dsl.PatternMetadata

import scala.reflect.ClassTag

class PatternGeneratorTest extends FlatSpec with Matchers with PropertyChecks {
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
    gen.build("doubleSensor1 > 0 for 30 sec", 0.0, fieldsClasses).right.value shouldBe a[(_, PatternMetadata)]
    gen.build("doubleSensor1 > 0 or longSensor = 0", 0.0, fieldsClasses).right.value shouldBe a[(_, PatternMetadata)]
    gen.build("doubleSensor1 > 0 and intSensor = 0", 0.0, fieldsClasses).right.value shouldBe a[(_, PatternMetadata)]
    gen
      .build("(doubleSensor1 + intSensor as float64) >= 10", 0.0, fieldsClasses)
      .right
      .value shouldBe a[(_, PatternMetadata)]
    gen
      .build("(doubleSensor1 + intSensor as float64) >= 10", 0.0, fieldsClasses)
      .right
      .value shouldBe a[(_, PatternMetadata)]
    gen
      .build("avgOf(doubleSensor1, doubleSensor2) >= 10 for 5 min >= 100 ms", 0.0, fieldsClasses)
      .right
      .value shouldBe a[(_, PatternMetadata)]
    gen
      .build("sin(doubleSensor1) >= 0.5 for 5 min andThen intSensor > 42", 0.0, fieldsClasses)
      .right
      .value shouldBe a[(_, PatternMetadata)]
    gen
      .build("tan(doubleSensor1) >= 1 for 5 hr andThen avg(doubleSensor2, 3 sec) > 42", 0.0, fieldsClasses)
      .right
      .value shouldBe a[(_, PatternMetadata)]
    gen
      .build("count(doubleSensor1, 4 sec) * sum(doubleSensor2, 3 sec) < 9", 0.0, fieldsClasses)
      .right
      .value shouldBe a[(_, PatternMetadata)]
    gen
      .build("lag(doubleSensor1, 10 sec) > doubleSensor1", 0.0, fieldsClasses)
      .right
      .value shouldBe a[(_, PatternMetadata)]
    //gen.build("boolSensor = true andThen boolSensor != false", 0.0, fieldsClasses).right.value shouldBe a[(Pattern[TestEvent, _, _], PatternMetadata)]
  }

  "Pattern generator" should "not generate invalid patterns" in {
    gen.build("boolSensor > 0 for 30 sec", 0.0, fieldsClasses).left.value shouldBe a[Throwable]
  }

  "Pattern generator" should "not build invalid patterns" in {
    // Assert argument must be boolean
    a[RuntimeException] should be thrownBy gen.generatePattern(Assert(Constant(1.0)))
  }

  "Casts" should "be performed" in {
    gen.build("doubleSensor1 as int32 > 0", 0.0, fieldsClasses).right.value shouldBe a[(_, PatternMetadata)]
    gen.build("doubleSensor1 as int64 > 0", 0.0, fieldsClasses).right.value shouldBe a[(_, PatternMetadata)]
    gen.build("doubleSensor1 as boolean = true", 0.0, fieldsClasses).right.value shouldBe a[(_, PatternMetadata)]
  }
}
