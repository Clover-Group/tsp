package ru.itclover.tsp.core
import cats.syntax.foldable._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Pattern.IdxExtractor._
import ru.itclover.tsp.core.Pattern.{IdxExtractor, QI}

import scala.language.higherKinds

// TODO Rename to FunctionP(attern)?
/** Simple Pattern */
trait SimplePatternLike[Event, T] extends Pattern[Event, SimplePState[T], T] {
  def idxExtractor: IdxExtractor[Event]
  val f: Event => Result[T]
  private val maxSegmentSize = 100000000 // todo make some di here

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: SimplePState[T],
    events: Cont[Event]
  ): F[SimplePState[T]] = {
    val (lastElement, newQueue) = events.foldLeft(Option.empty[IdxValue[T]] -> oldState.queue) {
      case ((None, queue), e) => {
        val value = f(e)
        val idx = e.index(idxExtractor)
        Some(IdxValue(idx, idx, value)) -> queue
      }
      case ((Some(x @ IdxValue(start, end@_, prevValue)), queue), e) => {
        val value = f(e)
        val idx = e.index(idxExtractor)
        // if new value is the same as previous than just expand segment
        if (value.equals(prevValue) && (idx - start <= maxSegmentSize)) {
          Some(IdxValue(start, idx, value)) -> queue
        } else {
          // otherwise put previous segment to the queue and start new segment
          Some(IdxValue(idx, idx, value)) -> queue.enqueue(x)
        }
      }
    }
    // Add last element if exist
    val finalQueue = lastElement.map(t => newQueue.enqueue(t)).getOrElse(newQueue)

    Monad[F].pure(SimplePState(finalQueue))
  }

}

case class SimplePattern[Event: IdxExtractor, T](override val f: Event => Result[T])
    extends SimplePatternLike[Event, T] {
  override def initialState(): SimplePState[T] = SimplePState(PQueue.empty)

  override def idxExtractor: IdxExtractor[Event] = implicitly
}

case class SimplePState[T](override val queue: QI[T]) extends PState[T, SimplePState[T]] {
  override def copyWith(queue: QI[T]): SimplePState[T] = this.copy(queue = queue)
}

case class ConstPattern[Event: IdxExtractor, T](value: Result[T]) extends SimplePatternLike[Event, T] {
  override val f: Event => Result[T] = _ => value
  override def initialState(): SimplePState[T] = SimplePState(PQueue.empty)

  override def idxExtractor: IdxExtractor[Event] = implicitly
}
