package ru.itclover.tsp.v2
import cats.implicits._
import cats.{Functor, Id, Monad}
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.io.TimeExtractor.GetTime
import ru.itclover.tsp.v2.Extract.IdxExtractor._
import ru.itclover.tsp.v2.Extract.{Idx, IdxExtractor, _}

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.language.higherKinds

//todo use Apache Arrow here. A lot of optimizations can be done.

object Extract {

  type Idx = Int
  type QI[T] = Queue[IdxValue[T]]

  type Result[T] = Option[T]

  object Result {
    def fail[T]: Result[T] = Fail
    def succ[T](t: T): Result[T] = Succ(t)
  }
  type Fail = None.type
  val Fail: Result[Nothing] = None
  type Succ[+T] = Some[T]

  object Succ {
    def apply[T](t: T): Succ[T] = Some(t)
    def unapply[T](arg: Succ[T]): Option[T] = Some.unapply(arg)
  }

  trait IdxExtractor[Event] extends Serializable {
    def apply(e: Event): Idx
  }

  object IdxExtractor {
    implicit class GetIdx[T](val event: T) extends AnyVal {
      def index(implicit te: IdxExtractor[T]): Idx = te.apply(event)
    }

    def of[E](f: E => Idx): IdxExtractor[E] = new IdxExtractor[E] {
      override def apply(e: E): Idx = f(e)
    }
  }
}

case class IdxValue[+T](index: Idx, value: Result[T])

trait PState[T, +Self <: PState[T, _]] {
  def queue: QI[T]
  def copyWithQueue(queue: QI[T]): Self
}

trait AddToQueue[F[_]] {
  def addToQueue[T](events: F[T], queue: Queue[T]): Queue[T]
}

object AddToQueue {

  def apply[T[_]: AddToQueue]: AddToQueue[T] = implicitly[AddToQueue[T]]

  implicit val idInstance: AddToQueue[Id] = new AddToQueue[Id] {
    override def addToQueue[T](events: Id[T], queue: Queue[T]): Queue[T] = queue.enqueue(events)
  }

  implicit val seqInstance: AddToQueue[Seq] = new AddToQueue[Seq] {
    override def addToQueue[T](events: Seq[T], queue: Queue[T]): Queue[T] = queue ++ events
  }

  implicit val listInstance: AddToQueue[List] = new AddToQueue[List] {
    override def addToQueue[T](events: List[T], queue: Queue[T]): Queue[T] = queue ++ events
  }
}

trait Pattern[Event, T, S <: PState[T, S], F[_], Cont[_]] extends ((S, Cont[Event]) => F[S]) with Serializable {

  def initialState(): S
}

/** AndThen  */
//We lose T1 and T2 in output for performance reason only. If needed outputs of first and second stages can be returned as well
case class AndThenPattern[Event, T1, T2, State1 <: PState[T1, State1], State2 <: PState[T2, State2], F[_]: Monad, Cont[
  _
]](
  first: Pattern[Event, T1, State1, F, Cont],
  second: Pattern[Event, T2, State2, F, Cont]
) extends Pattern[Event, (Idx, Idx), AndThenPState[T1, T2, State1, State2], F, Cont] {

  def apply(
    oldState: AndThenPState[T1, T2, State1, State2],
    event: Cont[Event]
  ): F[AndThenPState[T1, T2, State1, State2]] = {

    val firstF = first.apply(oldState.first, event)
    val secondF = second.apply(oldState.second, event)

    for (newFirstState  <- firstF;
         newSecondState <- secondF)
      yield {
        // process queues
        val (updatedFirstQueue, updatedSecondQueue, finalQueue) =
          process(newFirstState.queue, newSecondState.queue, oldState.queue)

        AndThenPState(
          newFirstState.copyWithQueue(updatedFirstQueue),
          newSecondState.copyWithQueue(updatedSecondQueue),
          finalQueue
        )
      }
  }

  override def initialState(): AndThenPState[T1, T2, State1, State2] =
    AndThenPState(first.initialState(), second.initialState(), Queue.empty)

  private def process(firstQ: QI[T1], secondQ: QI[T2], totalQ: QI[(Idx, Idx)]): (QI[T1], QI[T2], QI[(Idx, Idx)]) = {

    @tailrec
    def inner(first: QI[T1], second: QI[T2], total: QI[(Idx, Idx)]): (QI[T1], QI[T2], QI[(Idx, Idx)]) = {

      def default: (QI[T1], QI[T2], QI[(Idx, Idx)]) = (first, second, total)

      first.headOption match {
        // if any of parts is empty -> do nothing
        case None => default
        // if first part is Failure (== None) then return None as a result
        case Some(x @ IdxValue(_, Fail)) =>
          inner(first.dequeue._2, second, total.enqueue(x.asInstanceOf[IdxValue[(Idx, Idx)]])) // small optimization
        case Some(IdxValue(index1, _)) =>
          second.headOption match {
            // if any of parts is empty -> do nothing
            case None => default
            // if that's an late event from second queue, just skip it
            case Some(IdxValue(index2, _)) if index2 < index1 => //todo < or <= ?
              inner(first, second.dequeue._2, total)
            // if second part is Failure return None as a result
            case Some(IdxValue(_, Fail)) =>
              inner(first.dequeue._2, second, total.enqueue(IdxValue(index1, Fail)))
            // if both first and second stages a Success then return Success
            case Some(IdxValue(index2, Succ(_))) if index2 >= index1 =>
              inner(first.dequeue._2, second.dequeue._2, total.enqueue(IdxValue(index1, Succ(index1 -> index2))))
          }
      }
    }

    inner(firstQ, secondQ, totalQ)
  }
}

case class AndThenPState[T1, T2, State1 <: PState[T1, State1], State2 <: PState[T2, State2]](
  first: State1,
  second: State2,
  override val queue: QI[(Idx, Idx)]
) extends PState[(Idx, Idx), AndThenPState[T1, T2, State1, State2]] {

  override def copyWithQueue(queue: QI[(Idx, Idx)]): AndThenPState[T1, T2, State1, State2] = this.copy(queue = queue)
}

object AndThenPState {}

/** Couple Pattern */

class CouplePattern[Event, State1 <: PState[T1, State1], State2 <: PState[T2, State2], T1, T2, T3, F[_]: Monad, Cont[_]](
  left: Pattern[Event, T1, State1, F, Cont],
  right: Pattern[Event, T2, State2, F, Cont]
)(
  func: (Result[T1], Result[T2]) => Result[T3]
) extends Pattern[Event, T3, CouplePState[State1, State2, T1, T2, T3], F, Cont] {
  override def apply(
    oldState: CouplePState[State1, State2, T1, T2, T3],
    events: Cont[Event]
  ): F[CouplePState[State1, State2, T1, T2, T3]] = {
    val leftF = left.apply(oldState.left, events)
    val rightF = right.apply(oldState.right, events)
    for (newLeftState  <- leftF;
         newRightState <- rightF) yield {
      // process queues
      val (updatedLeftQueue, updatedRightQueue, newFinalQueue) =
        processQueues(newLeftState.queue, newRightState.queue, oldState.queue)

      CouplePState(
        newLeftState.copyWithQueue(updatedLeftQueue),
        newRightState.copyWithQueue(updatedRightQueue),
        newFinalQueue
      )
    }
  }

  private def processQueues(firstQ: QI[T1], secondQ: QI[T2], totalQ: QI[T3]): (QI[T1], QI[T2], QI[T3]) = {

    @tailrec
    def inner(first: QI[T1], second: QI[T2], total: QI[T3]): (QI[T1], QI[T2], QI[T3]) = {

      def default: (QI[T1], QI[T2], QI[T3]) = (first, second, total)

      (first.headOption, second.headOption) match {
        // if any of parts is empty -> do nothing
        case (_, None)                                                => default
        case (None, _)                                                => default
        case (Some(IdxValue(idx1, val1)), Some(IdxValue(idx2, val2))) =>
          // we emit result only if results on left and right sides come at the same time
          if (idx1 == idx2) {
            val result: Result[T3] = func(val1, val2)
            inner(first.dequeue._2, second.dequeue._2, total.enqueue(IdxValue(idx1, result)))
            // otherwise skip results from one of sides
          } else if (idx1 < idx2) {
            inner(first.dequeue._2, second, total)
          } else {
            inner(first, second.dequeue._2, total)
          }
      }
    }

    inner(firstQ, secondQ, totalQ)
  }

  override def initialState(): CouplePState[State1, State2, T1, T2, T3] =
    CouplePState(left.initialState(), right.initialState(), Queue.empty)
}

case class CouplePState[State1 <: PState[T1, State1], State2 <: PState[T2, State2], T1, T2, T3](
  left: State1,
  right: State2,
  override val queue: QI[T3]
) extends PState[T3, CouplePState[State1, State2, T1, T2, T3]] {
  override def copyWithQueue(queue: QI[T3]): CouplePState[State1, State2, T1, T2, T3] = this.copy(queue = queue)
}

case object CouplePState {}

/** Simple Pattern */
class SimplePattern[Event: IdxExtractor, T, F[_]: Monad, Cont[_]: Functor: AddToQueue](f: Event => Result[T])
    extends Pattern[Event, T, SimplePState[T], F, Cont] {
  override def apply(oldState: SimplePState[T], events: Cont[Event]): F[SimplePState[T]] = {
    val addToQueue: AddToQueue[Cont] = implicitly[AddToQueue[Cont]]
    Monad[F].pure(SimplePState(addToQueue.addToQueue(events.map(e => IdxValue(e.index, f(e))), oldState.queue)))
  }
  override def initialState(): SimplePState[T] = SimplePState(Queue.empty)
}

case class SimplePState[T](override val queue: QI[T]) extends PState[T, SimplePState[T]] {
  override def copyWithQueue(queue: QI[T]): SimplePState[T] = this.copy(queue = queue)
}

case class ConstPattern[Event: IdxExtractor, T, F[_]: Monad, Cont[_]: Functor: AddToQueue](t: T)
    extends SimplePattern[Event, T, F, Cont](_ => Result.succ(t))

/** Timer Pattern */
case class Timer[Event: IdxExtractor: TimeExtractor, S <: PState[T, S], T, F[_]: Monad, Cont[_]: Functor: AddToQueue](
  inner: Pattern[Event, T, S, F, Cont],
  window: Window
) extends Pattern[Event, T, TimerPState[S, T], F, Cont] {
  override def apply(state: TimerPState[S, T], event: Cont[Event]): F[TimerPState[S, T]] = {

    val updatedIdxTimeQueue =
      AddToQueue[Cont].addToQueue(event.map(event => event.index -> event.time), state.indexTimeMap)

    inner
      .apply(state.inner, event)
      .map(
        newInnerState => processQueue(TimerPState(newInnerState, state.queue, state.windowQueue, updatedIdxTimeQueue))
      )
  }

  private def processQueue(timerState: TimerPState[S, T]): TimerPState[S, T] = {
    import scala.Ordering.Implicits._

    @tailrec
    def rollMap(idx: Idx, q: Queue[(Idx, Time)]): (Time, Queue[(Idx, Time)]) = {
      assert(q.nonEmpty, "IdxTimeMap should not be empty!")
      q.dequeue match {
        case ((index, time), newq) =>
          if (index == idx) (time, newq)
          else if (index < idx) rollMap(idx, newq)
          else sys.error("Invalid state!")
      }
    }

    def takeWhileFromQueue[A](queue: Queue[A])(predicate: A => Boolean = (x: A) => true): (Queue[A], Queue[A]) = {
      @tailrec
      def inner(result: Queue[A], q: Queue[A]): (Queue[A], Queue[A]) =
        q.dequeueOption match {
          case Some((x, newQueue)) if predicate(x) => inner(result.enqueue(x), newQueue)
          case _                                   => (result, q)
        }

      inner(Queue.empty, queue)
    }

    def updateWindow(index: Idx, value: Result[T], newInnerResultTime: Time, windowQueue: Queue[(Idx, Time)]) = {
      value match {
        // clean queue in case of fail. Return fails for all events in queue
        case Fail =>
          val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue)(_ => true)
          val newResults: QI[T] = outputs.map { case (idx, _) => IdxValue(idx, Result.fail[T]) }
          (updatedWindowQueue, newResults)
        // in case of Success we need to return Success for all events in window older than window size.
        case Succ(_) => {

          val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue) {
            case (_, time) => time.plus(window) < newInnerResultTime
          }

          val windowQueueWithNewEvent = updatedWindowQueue.enqueue((index, newInnerResultTime))
          val newResults: QI[T] = outputs.map { case (idx, _) => IdxValue(idx, value) }
          (windowQueueWithNewEvent, newResults)
        }
      }
    }

    @tailrec
    def inner(tps: TimerPState[S, T]): TimerPState[S, T] =
      tps.inner.queue.dequeueOption match {
        case None => tps
        case Some((IdxValue(index, value), updatedQueue)) => {

          val (newInnerResultTime, updatedIdxTimeMap) = rollMap(index, tps.indexTimeMap)

          val windowQueue = tps.windowQueue

          val (updatedWindowQueue, newResults) = updateWindow(index, value, newInnerResultTime, windowQueue)

          inner(
            TimerPState(
              tps.inner.copyWithQueue(updatedQueue),
              tps.queue.enqueue(newResults),
              updatedWindowQueue,
              updatedIdxTimeMap
            )
          )
        }
      }

    inner(timerState)
  }
  override def initialState(): TimerPState[S, T] =
    TimerPState(inner = inner.initialState(), Queue.empty, Queue.empty, Queue.empty)
}

case class TimerPState[S <: PState[T, S], T](
  inner: S,
  override val queue: QI[T],
  windowQueue: Queue[(Idx, Time)],
  indexTimeMap: Queue[(Idx, Time)]
) extends PState[T, TimerPState[S, T]] {
  override def copyWithQueue(queue: QI[T]): TimerPState[S, T] = this.copy(queue = queue)
}

/** Window Like Pattern */
//case class DoublePattern[Event, F[_]: Monad](f: Event => Double) extends Pattern[Event, NoState, Double, F] {
//  override def apply(noState: NoState, e: Event): F[(NoState, PatternResult[Double])] =
//    Monad[F].point(noState -> Success(f(e)))
//}
//case class NoState private () extends Serializable
//
//object NoState {
//  val instance = new NoState()
//
//  implicit val semigroup: Semigroup[NoState] = Semigroup.instance[NoState] { case (_, _) => instance }
//  implicit def creatable[From]: Creatable[From, NoState] = Creatable.instance(NoState.instance)
//}
//

