package ru.itclover.tsp.core
import cats.syntax.foldable._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Pattern.IdxExtractor._
import ru.itclover.tsp.core.Pattern.{IdxExtractor, QI}

import scala.language.higherKinds

// TODO Rename to FunctionP(attern)?
/** Simple Pattern */
trait SimplePatternLike[Event, T] extends Pattern[Event, SimplePState.type, T] {
  def idxExtractor: IdxExtractor[Event]
  val f: Event => Result[T]

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: SimplePState.type,
    oldQueue: PQueue[T],
    events: Cont[Event]
  ): F[(SimplePState.type, PQueue[T])] = {
    val (lastElement, newQueue) = events.foldLeft(Option.empty[IdxValue[T]] -> oldQueue) {
      case ((None, queue), e) => {
        val value = f(e)
        val idx = e.index(idxExtractor)
        Some(IdxValue(idx, idx, value)) -> queue
      }
      case ((Some(x @ IdxValue(start, end @ _, prevValue)), queue), e) => {
        val value = f(e)
        val idx = e.index(idxExtractor)
        // if new value is the same as previous than just expand segment
        if (value.equals(prevValue)) {
          Some(IdxValue(start, idx, value)) -> queue
        } else {
          // otherwise put previous segment to the queue and start new segment
          Some(IdxValue(idx, idx, value)) -> queue.enqueue(x)
        }
      }
    }
    // Add last element if exist
    val finalQueue = lastElement.map(t => newQueue.enqueue(t)).getOrElse(newQueue)

    Monad[F].pure(SimplePState -> finalQueue)
  }

  override def initialState(): SimplePState.type = SimplePState
}

case class SimplePattern[Event: IdxExtractor, T](override val f: Event => Result[T])
    extends SimplePatternLike[Event, T] {

  override def idxExtractor: IdxExtractor[Event] = implicitly
}

case object SimplePState

case class ConstPattern[Event: IdxExtractor, T](value: Result[T]) extends SimplePatternLike[Event, T] {
  override val f: Event => Result[T] = _ => value
  override def idxExtractor: IdxExtractor[Event] = implicitly
}
