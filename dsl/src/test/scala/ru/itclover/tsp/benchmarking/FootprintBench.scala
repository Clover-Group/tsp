package ru.itclover.tsp.benchmarking

import cats._
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.dsl.v2.{ASTPatternGenerator, TestEvents}
import ru.itclover.tsp.core._

import scala.collection.mutable.ArrayBuffer
import scala.language.reflectiveCalls
import scala.reflect.ClassTag

class FootprintBench extends FlatSpec with Matchers {

  import TestEvents._

  val fieldsClasses = Map(
    'intSensor     -> ClassTag.Int,
    'longSensor    -> ClassTag.Long,
    'boolSensor    -> ClassTag.Boolean,
    'doubleSensor1 -> ClassTag.Double,
    'doubleSensor2 -> ClassTag.Double
  )

  def process[T, S <: PState[T, S]](pattern: Pattern[TestEvent, S, T], events: Seq[TestEvent]): Long = {
    val start = System.nanoTime()
    val sm = StateMachine[Id]
    val initialState = pattern.initialState()
    val collect = new ArrayBuffer[Long](events.size)
    sm.run(pattern, events, initialState, (x: IdxValue[T]) => collect += x.index, 1000)
    (System.nanoTime() - start) / 1000000
  }

  def repeat[T, S <: PState[T, S]](times: Int, amount: Int, pattern: Pattern[TestEvent, S, T]): Long = {
    val events = (1 to amount).map(l => TestEvent(l.toLong * 1000, 1, 1, boolSensor = true, 1.0, 2.0)).seq
    val ts = (1 to times).map(_ => { val t = process(pattern, events); t }).sum
    ts / times
  }

  it should "process pattern by ASTPatternGenerator correctly" in {

    val gen = new ASTPatternGenerator[TestEvent, Symbol, Any]
    val expectedTime = 3000

    val patternString = gen
      .build(
        "intSensor > 0 for 720 sec",
        0.0,
        fieldsClasses
      )
      .right
      .get
      ._1
    val actualTime = repeat(5, 1000000, patternString)
    assert(actualTime > expectedTime)

  }

}
