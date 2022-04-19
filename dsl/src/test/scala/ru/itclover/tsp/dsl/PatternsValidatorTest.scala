package ru.itclover.tsp.dsl

import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.RawPattern

class PatternsValidatorTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks {
  import TestEvents._

  val patterns = Seq(
    "doubleSensor1 > doubleSensor2",
    "intSensor + longSensor > 100",
    "boolSensor = true and lag(intSensor) = 5",
    "stringSensor = 'string' and stringSensor != 'also a string'"
  )

  val fieldsTypes = Map(
    "doubleSensor1" -> "float64",
    "doubleSensor2" -> "float64",
    "floatSensor1"  -> "float32",
    "floatSensor2"  -> "float32",
    "longSensor"    -> "int64",
    "intSensor"     -> "int32",
    "shortSensor"   -> "int16",
    "byteSensor"    -> "int8",
    "boolSensor"    -> "boolean",
    "stringSensor"  -> "string",
    "anySensor"     -> "any"
  )

  "Pattern validator" should "validate patterns" in {
    PatternsValidator
      .validate[TestEvent](patterns.zipWithIndex.map(pi => RawPattern(pi._2, pi._1, None)), fieldsTypes)
  }
}
