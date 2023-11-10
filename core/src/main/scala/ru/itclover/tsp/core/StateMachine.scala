package ru.itclover.tsp.core

import cats.{Monad, Traverse}
import cats.syntax.flatMap._
import com.typesafe.scalalogging.Logger

class StateMachine[F[_]: Monad: Traverse] {
  private val log = Logger("StateMachineV2")

  /** Runs stateMachine for given `pattern` and `events`. Use `seedState` as an initial State. Calls `consume` for every
    * output result.
    *
    * @param pattern
    *   to be applied to `events`
    * @param events
    *   bunch of events to be processed
    * @param seedState
    *   initial state
    * @param consume
    *   function to be called for each output
    * @param groupSize
    *   internal chunks size. Default is 1000
    * @tparam Event
    *   intput events type
    * @tparam Out
    *   output type
    * @tparam State
    *   internal State type
    * @return
    *   F[State] there State's queue is empty. It allows to avoid memory leaks.
    */
  def run[Event, Out, State](
    pattern: Pattern[Event, State, Out],
    events: Iterable[Event],
    seedState: State,
    consume: IdxValue[Out] => F[Unit] = (_: IdxValue[Out]) => Monad[F].pure(()),
    groupSize: Int = 100000
  ): F[State] = {

    var counter = 0
    import cats.instances.list._

    val finalState: F[State] = events.iterator
      .grouped(groupSize)
      .foldLeft(Monad[F].pure(seedState)) { case (state, evs) =>
        // log.debug(s"After $counter rows")
        counter += groupSize

        state
          .flatMap(s => pattern.apply[F, List](s, PQueue.empty, evs.toList))
          .flatMap {
            case (newState, newQueue) => {

              val allConsumed: F[Unit] = newQueue.toSeq.foldLeft(Monad[F].pure(())) { case (t, out) =>
                t.flatMap(_ => consume(out))
              }

              Monad[F].map(allConsumed)(_ => newState)
            }
          }
      }

    log.debug("Finished")
    finalState
  }

}

object StateMachine {

  def apply[F[_]: Monad: Traverse] = new StateMachine[F]
}
