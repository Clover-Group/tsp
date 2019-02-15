package ru.itclover.tsp.mappers

import java.lang

import cats.Id
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.Pattern.QI
import ru.itclover.tsp.v2.{PState, Pattern, StateMachine, Succ}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

//case class PatternFlatMapper[E, State <: PState[Inner, State], Inner, Out](
//  pattern: Pattern[E, State, Inner], //todo Why List?
//  mapResults: (E, Seq[Inner]) => Seq[Out],
//  eventsMaxGapMs: Long,
//  emptyEvent: E
//)(
//  implicit timeExtractor: TimeExtractor[E]
//) extends StatefulFlatMapper[E, (State, E), Out]
//    with Serializable {
//  var counter = 0
//
//  override def initialState = (pattern.initialState(), emptyEvent)
//
//  override def apply(event: E, stateAndPrevEvent: (State, E)) = {
//    counter += 1
//    println(f"${System.identityHashCode(this)}%18d Processing event #$counter")
//    val prevEvent = stateAndPrevEvent._2
//    val newState: State = if (doProcessOldState(event, prevEvent)) {
//      StateMachine[Id].run(pattern, List(event), stateAndPrevEvent._1)
//    } else {
//      StateMachine[Id]
//        .run(pattern, List(event), pattern.initialState()) // .. careful here, init state may need to create only 1 time
//    }
//    // TODO: Can be more than 1 result per 1 event and 1 pattern?
//    val nState = newState.copyWithQueue(newState.queue.takeRight(1))
//    val results = nState.queue.map(_.value).collect { case Succ(v) => v }
//    // Non failed results with events (for toIncidentsMapper) + state with previous event (to tract gaps in the data)
//    (mapResults(event, results), (nState, event))
//  }
//
//  /** Check is new event from same events time seq */
//  def doProcessOldState(currEvent: E, prevEvent: E) = {
//    if (prevEvent == emptyEvent) true
//    else timeExtractor(currEvent).toMillis - timeExtractor(prevEvent).toMillis < eventsMaxGapMs
//  }
//}
//
//object PatternFlatMapper {
//  val currentEventTsMetric = "currentEventTs"
//}

case class PatternProcessor[E, State <: PState[Inner, State], Inner, Out](
  pattern: Pattern[E, State, Inner], //todo Why List?
  mapResults: (E, Seq[Inner]) => Seq[Out],
  eventsMaxGapMs: Long,
  emptyEvent: E
)(
  implicit timeExtractor: TimeExtractor[E]
) {
  var lastState = pattern.initialState()

  def process(
    key: String,
    elements: Iterable[E],
    out: Collector[Out]
  ): Unit = {
    // Split the different time sequences if they occurred in the same time window
    val sequences = PatternProcessor.splitByCondition(
      elements.toList,
      (next: E, prev: E) => timeExtractor(next).toMillis - timeExtractor(prev).toMillis > eventsMaxGapMs
    )
    val states = StateMachine[Id].run(pattern, sequences.head, lastState) :: sequences.tail.map(
      StateMachine[Id].run(pattern, _, pattern.initialState())
    )
    lastState = states.last
    val results = states.map(_.queue).foldLeft(List.empty[Inner]) { (acc: List[Inner], q: QI[Inner]) =>
      acc ++ q.map(_.value).collect { case Succ(v) => v }
    }
    if (elements.nonEmpty)
      mapResults(elements.head, results).foreach(out.collect)
  }
}

object PatternProcessor {

  /**
    * Splits a list into a list of fragments, the boundaries are determined by the given predicate
    * E.g. `splitByCondition(List(1,2,3,5,8,9,12), (x, y) => (x - y) > 2) == List(List(1,2,3,5),List(8,9),List(12)`
    * @param elements initial sequence
    * @param pred condition between the next and previous elements (in this order)
    * @tparam T Element type
    * @return List of chunks
    */
  def splitByCondition[T](elements: List[T], pred: (T, T) => Boolean): List[Seq[T]] = {
    val results = ListBuffer(ListBuffer(elements.head))
    if (elements.length < 2) {
      List(elements)
    } else {
      elements.sliding(2).foreach { e =>
        val prev = e(0)
        val cur = e(1)
        if (pred(cur, prev)) {
          results += ListBuffer(cur)
        } else {
          results.last += cur
        }
      }
      results.map(_.toSeq).toList
    }
  }
  val currentEventTsMetric = "currentEventTs"
}
