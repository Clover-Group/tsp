package ru.itclover.tsp.benchmarking

import cats._
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.dsl.v2.{ASTPatternGenerator, TestEvents}
import ru.itclover.tsp.v2._

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

  def process[T, S <: PState[T, S]](pattern: Pattern[TestEvent, S, T]): Unit = {
    val events = (1 to 10000000).map(l => TestEvent(l.toLong * 1000, 1, 1, true, 1.0, 2.0)).seq
    val sm = StateMachine[Id]
    val initialState = pattern.initialState()
    val collect = new ArrayBuffer[Long]()
    val actState = sm.run(pattern, events, initialState, (x: IdxValue[T]) => collect += x.index)

    println(org.openjdk.jol.info.GraphLayout.parseInstance(actState).toFootprint)
  }

  it should "process ConstPattern correctly" in {

    val gen = new ASTPatternGenerator[TestEvent, Symbol, Any]
    val pattern = gen.build("intSensor > 0 for 720 sec", 0.0, fieldsClasses).right.get._1

    process(pattern)
    // Instantiate an output state manually
    val eventsQueue = PQueue(IdxValue(1, Result.succ(0)))
    val expState = SimplePState[Int](eventsQueue)

    // Temporary, until custom Equality[IdxValue] is implemented
//    expState.queue.length shouldBe actState.queue.length
  }

}
