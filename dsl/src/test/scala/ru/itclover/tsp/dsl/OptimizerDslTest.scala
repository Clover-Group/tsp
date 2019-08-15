package ru.itclover.tsp.dsl

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor}
import org.scalatest.EitherValues._
import ru.itclover.tsp.dsl.{ASTPatternGenerator, PatternMetadata}
import ru.itclover.tsp.core.Pattern

import scala.reflect.ClassTag
import ru.itclover.tsp.core.CouplePattern
import org.scalacheck.Test
import ru.itclover.tsp.core.optimizations.Optimizer
import ru.itclover.tsp.core.SimplePattern
import ru.itclover.tsp.core.MapPattern
import ru.itclover.tsp.core.Pat

class OptimizerDslTest extends FlatSpec with Matchers with PropertyChecks {
  import TestEvents._

  val fieldsClasses = Map(
    'intSensor     -> ClassTag.Int,
    'longSensor    -> ClassTag.Long,
    'boolSensor    -> ClassTag.Boolean,
    'doubleSensor1 -> ClassTag.Double,
    'doubleSensor2 -> ClassTag.Double
  )

  val gen = new ASTPatternGenerator[TestEvent, Symbol, Any]

  "Optimizer" should "optimize pattern build from string" in {

    val nonOptPattern = gen.build("doubleSensor1 > 1.0 or longSensor = 2.0", 0.0, fieldsClasses).right.value._1
    nonOptPattern shouldBe a[MapPattern[_, _, _, _]]
    val inner: Pat[_, _] = nonOptPattern match {
      case Pat(MapPattern(inner)) => inner
    }
    inner shouldBe a[CouplePattern[_, _, _, _, _, _]]

    val optimizer = new Optimizer[TestEvent]()
    val optimizedPattern = optimizer.optimize(nonOptPattern)
    optimizedPattern shouldBe a[SimplePattern[_, _]]

  }
}
