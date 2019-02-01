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
//
  def pattern: Pattern[Event, State, Out]

  def initialState

//  def process(event: Event, ):  Seq[State] = {
//    log.debug(s"Search for patterns in: $event")
//    // new state is adding every time to account every possible outcomes considering terminal nature of phase mappers
//    val oldStatesWithOneInitialState = oldStates :+ pattern.initialState
//
//    val stateToResult = pattern.curried(event)
//
//    val resultsAndStates = oldStatesWithOneInitialState.map(stateToResult)
//
//    val (toEmit, newStates) = resultsAndStates.span(_._1.isTerminal)
//
//    // TODO: Move to SMM or to Writer monad in Phases
//    if (isDebug) {
//      val emitsLog = toEmit.map(resAndSt => pattern.format(event, resAndSt._2) + " emits " + resAndSt._1)
//      if (emitsLog.nonEmpty) log.debug(s"Results to emit: ${emitsLog.mkString("\n", "\n", "")}")
//      log.debug(s"States on hold: ${newStates.size}")
//    }
//
//    toEmit.map(_._1).asInstanceOf[Seq[TerminalResult[Out]]] -> newStates.map(_._2)
//  }

}
