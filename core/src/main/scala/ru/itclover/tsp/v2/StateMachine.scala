package ru.itclover.tsp.v2

import cats.Monad
import cats.syntax.functor._
import cats.syntax.flatMap._
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.v2.Pattern.QI

import scala.language.higherKinds

object StateMachine {

  private val log = Logger("StateMachineV2")

  def run[Event, Out, State <: PState[Out, State], F[_]](
    pattern: Pattern[Event, Out, State, F, List],
    events: Iterable[Event],
    previousState: State,
    groupSize: Int = 1000
  )(implicit F: Monad[F]): F[State] = {

    var counter = 0

    val finalState: F[State] = events.grouped(groupSize).foldLeft(F.pure(previousState)) {
      case (state, evs) => {
        log.debug(s"After $counter rows")
        counter += groupSize
        state.flatMap(s => pattern.apply(s, evs.toList))
      }
    }

    log.debug("Finished")
    finalState
  }

}
