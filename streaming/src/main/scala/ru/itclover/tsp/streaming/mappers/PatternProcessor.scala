package ru.itclover.tsp.streaming.mappers

import cats.Id
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.{Time, _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class PatternProcessor[E: TimeExtractor, State, Out: Merger](
  pattern: Pattern[E, State, Out],
  patternIdAndSubunit: (Int, Int),
  eventsMaxGapMs: Long,
  initialState: () => State,
  writeTimeLogs: Boolean
):

  private val log = Logger("PatternLogger")
  private var lastState: State = scala.compiletime.uninitialized
  private var lastTime: Time = Time(0)
  private val timeExtractor = implicitly[TimeExtractor[E]]
  log.debug(s"pattern: $pattern")

  def map(
    elements: Iterable[E]
  ): Iterable[Out] =
    if elements.isEmpty then
      log.info("No elements to proccess")
      return List.empty

    val firstElement = elements.head
    // if the last event occurred so long ago, clear the state
    val delta = timeExtractor(firstElement).toMillis - lastTime.toMillis
    if lastState == null || delta > eventsMaxGapMs || delta < 0 then lastState = initialState()

    // Split the different time sequences if they occurred in the same time window
    val sequences = PatternProcessor.splitByCondition(elements.toSeq)((next, prev) =>
      timeExtractor(next).toMillis - timeExtractor(prev).toMillis > eventsMaxGapMs
    )

    val machine = StateMachine[Id]

    val data = mutable.ListBuffer.empty[Out]

    val consume: IdxValue[Out] => Unit = DataConsumer(data)

    val seedStates = lastState +: LazyList.continually(initialState())

    // this step has side-effect = it calls `consume` for each output event. We need to process
    // events sequentually, that's why I use foldLeft here
    lastState = sequences.zip(seedStates).foldLeft(initialState()) { case (_, (events, seedState)) =>
      machine.run(pattern, patternIdAndSubunit, events, seedState, consume, writeTimeLogs = writeTimeLogs)
    }

    lastTime = elements.lastOption.map(timeExtractor(_)).getOrElse(Time(0))

    data.toList

  def getState: State = lastState

trait Merger[T]:
  def isWaitState(item: T): Boolean
  def merge(first: T, second: T): T

object IncidentMerger extends Merger[Incident]:
  def isWaitState(item: Incident): Boolean = item.segment.isWait
  def merge(first: Incident, second: Incident): Incident = IncidentInstances.semigroup.combine(first, second)

case class DataConsumer[Out](outBuffer: mutable.ListBuffer[Out])(implicit merger: Merger[Out])
    extends (IdxValue[Out] => Unit):
  var waitState: Option[Out] = None

  override def apply(idxValue: IdxValue[Out]) = idxValue match
    case IdxValue(start, end, value) =>
      waitState match
        case None =>
          value match
            case Succ(t) =>
              if merger.isWaitState(t) then waitState = Some(t)
              else outBuffer.append(t)
            case _ => /* do nothing */
        case Some(wait) =>
          value match
            case Succ(t) =>
              if merger.isWaitState(t) then waitState = Some(merger.merge(wait, t))
              else outBuffer.append(merger.merge(wait, t))
            case _ => waitState = None

object PatternProcessor:

  val currentEventTsMetric = "currentEventTs"

  /** Splits a list into a list of fragments, the boundaries are determined by the given predicate E.g.
    * `splitByCondition(List(1,2,3,5,8,9,12), (x, y) => (x - y) > 2) == List(List(1,2,3,5),List(8,9),List(12)`
    *
    * @param elements
    *   initial sequence
    * @param pred
    *   condition between the next and previous elements (in this order)
    * @tparam T
    *   Element type
    * @return
    *   List of chunks
    */
  def splitByCondition[T](elements: Seq[T])(pred: (T, T) => Boolean): List[Seq[T]] =
    if elements.length < 2 then List(elements)
    else
      val results = ListBuffer(ListBuffer(elements.head))
      elements.sliding(2).foreach { e =>
        val prev = e.head
        val cur = e(1)
        if pred(cur, prev) then results += ListBuffer(cur)
        else results.lastOption.getOrElse(sys.error("Empty result sequence - something went wrong")) += cur
      }
      results.map(_.toSeq).toList
