package ru.itclover.tsp.v2

import cats.Monad
import cats.syntax.flatMap._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger

import scala.language.higherKinds

object StateMachine {

  def apply[F[_]: Monad] = new {

    private val log = Logger("StateMachineV2")

    def run[Event, Out, State <: PState[Out, State]](
      pattern: Pattern[Event, State, Out],
      events: Iterable[Event],
      previousState: State,
      groupSize: Int = 1000
    ): F[State] = {

      var counter = 0

      import cats.instances.list._

      val finalState: F[State] = events.grouped(groupSize).foldLeft(Monad[F].pure(previousState)) {
        case (state, evs) => {
          log.debug(s"After $counter rows")
          counter += groupSize
          state.flatMap(s => pattern.apply[F, List](s, evs.toList))
        }
      }

      log.debug("Finished")
      finalState
    }
  }

}

abstract class AbstractStateMachine[Event, Out, State <: PState[Out, State], F[_], Cont[_]] {
  private val isDebug = ConfigFactory.load().getBoolean("general.is-debug")
  private val log = Logger("AbstractPatternMapper")

  def pattern: Pattern[Event, State, Out]

  def initialState

}
