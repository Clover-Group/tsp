package ru.itclover.tsp.mappers

import cats.Id
import com.typesafe.scalalogging.Logger
import org.apache.flink.util.Collector
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.{Time, _}
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.optimizations.Optimizer

import scala.collection.mutable.ListBuffer

case class PatternProcessor[E, State, Inner, Out](
  pattern: Pattern[E, State, Inner],
  patternMaxWindow: Long,
  mapResults: E => Inner => Out,
  eventsMaxGapMs: Long
)(
  implicit timeExtractor: TimeExtractor[E],
  idxExtractor: IdxExtractor[E]
) {

  val optimizer: Optimizer[E] = new Optimizer[E]()
  val log = Logger("PatternLogger")
  var lastState: Optimizer.S[Out] = _
  var lastTime: Time = Time(0)
  log.debug(s"pattern: $pattern")

  def process(
    // key: String,
    elements: Iterable[E],
    out: Collector[Out]
  ): Unit = {
    if (elements.isEmpty) {
      log.info("No elements to proccess")
      return
    }

    //todo move it to another place
    val firstElement = elements.head
    val mapFunction = mapResults(firstElement) // do not inline!
    val mappedPattern: MapPattern[E, Inner, Out, State] = MapPattern(pattern)(in => Result.succ(mapFunction(in)))

    val optimizedPattern = new Optimizer[E].optimize(mappedPattern)
    val initialState = optimizedPattern.initialState()

    // if the last event occurred so long ago, clear the state
    if (lastState == null || timeExtractor(firstElement).toMillis - lastTime.toMillis > eventsMaxGapMs) {
      lastState = initialState
    }

    // Split the different time sequences if they occurred in the same time window
    val sequences = PatternProcessor.splitByCondition(elements.toList)(
      (next, prev) => timeExtractor(next).toMillis - timeExtractor(prev).toMillis > eventsMaxGapMs
    )

    val machine = StateMachine[Id]

    val consume: IdxValue[Out] => Unit = x => x.value.foreach(out.collect)

    val seedStates = lastState +: Stream.continually(initialState)

    // this step has side-effect = it calls `consume` for each output event. We need to process
    // events sequentually, that's why I use foldLeft here
    lastState = sequences.zip(seedStates).foldLeft(initialState) {
      case (_, (events, seedState)) => machine.run(optimizedPattern, events, seedState, consume)
    }

    lastTime = elements.lastOption.map(timeExtractor(_)).getOrElse(Time(0))
  }
}

object PatternProcessor {

  val currentEventTsMetric = "currentEventTs"

  /**
    * Splits a list into a list of fragments, the boundaries are determined by the given predicate
    * E.g. `splitByCondition(List(1,2,3,5,8,9,12), (x, y) => (x - y) > 2) == List(List(1,2,3,5),List(8,9),List(12)`
    *
    * @param elements initial sequence
    * @param pred condition between the next and previous elements (in this order)
    * @tparam T Element type
    * @return List of chunks
    */
  def splitByCondition[T](elements: List[T])(pred: (T, T) => Boolean): List[Seq[T]] =
    if (elements.length < 2) {
      List(elements)
    } else {
      val results = ListBuffer(ListBuffer(elements.head))
      elements.sliding(2).foreach { e =>
        val prev = e.head
        val cur = e(1)
        if (pred(cur, prev)) {
          results += ListBuffer(cur)
        } else {
          results.lastOption.getOrElse(sys.error("Empty result sequence - something went wrong")) += cur
        }
      }
      results.map(_.toSeq).toList
    }
}
