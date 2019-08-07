package ru.itclover.tsp.benchmarking

import cats._
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.dsl.v2.{ASTPatternGenerator, TestEvents}
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.aggregators.TimerPattern

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
    val actState = sm.run(pattern, events, initialState, (x: IdxValue[T]) => collect += x.index, 1000)
    (System.nanoTime() - start) / 1000000

//    println(org.openjdk.jol.info.GraphLayout.parseInstance(actState).toFootprint)
  }

  def repeat[T, S <: PState[T, S]](times: Int, amount: Int, pattern: Pattern[TestEvent, S, T]): Long = {
    val events = (1 to amount).map(l => TestEvent(l.toLong * 1000, 1, 1, true, 1.0, 2.0)).seq
    val ts = (1 to times).map(_ => {val t = process(pattern, events); println(t);t}).sum
    ts / times
  }

  it should "process ConstPattern correctly" in {

    implicit val patterns = new Patterns[TestEvent] {}
    import patterns._

    val gen = new ASTPatternGenerator[TestEvent, Symbol, Any]
    val patternString = gen.build("intSensor > 0 for 720 sec", 0.0, fieldsClasses).right.get._1
    println("String pattern: " + repeat(5, 1000000, patternString) )
//
//    val patternCouple = timer(patterns.assert(field(e => e.intSensor).gt(const(0))), Window(720000))
//    println("Couple pattern: " + repeat(10, 10000000, patternCouple) )
//
//    val patternMap = timer(patterns.assert(field(e => e.intSensor).map(_ > 0)), Window(720000))
//    println("Map pattern: " + repeat(10, 10000000, patternMap) )

//    val success = Result.succ(())
//    val patternMap2 =
//      timer(new SimplePattern((e: TestEvent) => if (e.intSensor > 0) success else Result.fail), Window(7200000))
//    println("Map pattern: " + repeat(10, 10000000, patternMap2) )

  }

}
