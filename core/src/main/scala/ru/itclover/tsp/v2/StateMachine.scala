package ru.itclover.tsp.v2
import cats.Monad
import cats.syntax.functor._
import cats.syntax.flatMap._
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.v2.Pattern.QI

import scala.language.higherKinds

object StateMachine {

  private val log = Logger("StateMachineV2")

  def run[Event, Out, S <: PState[Out, S], F[_]: Monad](
    pattern: Pattern[Event, Out, S, F, List],
    events: Iterable[Event],
    groupSize: Int = 1000
  ): F[QI[Out]] = {

    var counter = 0
    val initialState = pattern.initialState()

    val finalstate = events.grouped(groupSize).foldLeft(Monad[F].pure(initialState)) {
      case (state, evs) => {
        log.debug(s"After $counter rows")
        counter += groupSize
        state.flatMap(s => pattern.apply(s, evs.toList))
      }
    }

    log.debug("Finished")
    finalstate.map(_.queue)
  }

}
