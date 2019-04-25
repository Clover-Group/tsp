package ru.itclover.tsp.v2

import cats.Monad
import cats.syntax.flatMap._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger

import scala.language.higherKinds

object StateMachine {

  def apply[F[_]: Monad: cats.Traverse] = new {

    private val log = Logger("StateMachineV2")

    /**
      * Runs stateMachine for given `pattern` and `events`. Use `seedState` as an initial State.
      * Calls `consume` for every output result.
      *
      * @param pattern to be applied to `events`
      * @param events bunch of events to be processed
      * @param seedState initial state
      * @param consume function to be called for each output
      * @param groupSize internal chunks size. Default is 1000
      * @tparam Event intput events type
      * @tparam Out output type
      * @tparam State internal State type
      * @return F[State] there State's queue is empty. It allows to avoid memory leaks.
      */
    def run[Event, Out, State <: PState[Out, State]](
      pattern: Pattern[Event, State, Out],
      events: Iterable[Event],
      seedState: State,
      consume: IdxValue[Out] => F[Unit] = (_: IdxValue[Out]) => Monad[F].pure(()),
      groupSize: Int = 1000
    ): F[State] = {

      var counter = 0
      import cats.instances.list._

      val finalState: F[State] = events.iterator
        .grouped(groupSize)
        .foldLeft(Monad[F].pure(seedState)) {
          case (state, evs) =>
            log.debug(s"After $counter rows")
            counter += groupSize

            state
              .flatMap(s => pattern.apply[F, List](s, evs.toList))
              .flatMap { newState =>
                {

                  val outputs = newState.queue.toSeq
                  val drainedState = newState.copyWithQueue(PQueue.empty)
                  val allConsumed: F[Unit] = outputs.foldLeft(Monad[F].pure(())) {
                    case (t, out) => t.flatMap(_ => consume(out))
                  }

                  Monad[F].map(allConsumed)(_ => drainedState)
                }
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

  def initialState()

}
