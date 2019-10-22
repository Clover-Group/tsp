package ru.itclover.tsp.dsl

import org.scalatest.EitherValues._

import scala.reflect.ClassTag
import ru.itclover.tsp.core.CouplePattern
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.{CouplePattern, MapPattern, Pat, SimplePattern}
import ru.itclover.tsp.core.optimizations.Optimizer

import scala.reflect.ClassTag

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
