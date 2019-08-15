package ru.itclover.tsp.dsl.v2
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.core.io.AnyDecodersInstances._
import ru.itclover.tsp.dsl.PatternsValidator

class PatternsValidatorTest extends FlatSpec with Matchers with PropertyChecks {
  import TestEvents._

  val patterns = Seq(
    "doubleSensor1 > doubleSensor2",
    "intSensor + longSensor > 100",
    "boolSensor = true and lag(intSensor) = 5"
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
    val results = PatternsValidator
      .validate[TestEvent](patterns.zipWithIndex.map(pi => RawPattern(pi._2.toString, pi._1)), fieldsTypes)
  }
}
