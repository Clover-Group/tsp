package ru.itclover.tsp.core

import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import com.typesafe.scalalogging.Logger

case class SuccessStatPattern[Event, S, T](val patternId: Int, val inner: Pattern[Event, S, T])
    extends Pattern[Event, S, T] {

  val log = Logger[SuccessStatPattern[Event, S, T]]

  override def initialState(): S = inner.initialState()

  override def apply[F[_$1]: Monad, Cont[_$2]: Foldable: Functor](
    oldState: S,
    queue: PQueue[T],
    events: Cont[Event]
  ): F[(S, PQueue[T])] = {
    val innerResult = inner.apply[F, Cont](oldState, queue, events)
    for f <- innerResult
    yield
      val data = f._2.toSeq
      val successes = data.filter(x => x.value.isSuccess).foldLeft(0L)((acc, x) => acc + (x.end - x.start + 1))
      val failures = data.filter(x => x.value.isFail).foldLeft(0L)((acc, x) => acc + (x.end - x.start + 1))
      val total = successes + failures
      log.debug(
        s"Pattern ID $patternId: $successes successes, $failures failures out of $total (${successes * 100.0 / total} %)"
      )
      f
  }

}
