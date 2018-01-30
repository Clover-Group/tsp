package ru.itclover.streammachine.core

import ru.itclover.streammachine.phases.BooleanPhases.BooleanPhasesSyntax
import ru.itclover.streammachine.phases.CombiningPhases.CombiningPhasesSyntax
import ru.itclover.streammachine.phases.MonadPhases.MonadPhasesSyntax
import ru.itclover.streammachine.phases.NumericPhases.NumericPhasesSyntax
import ru.itclover.streammachine.phases.TimePhases.TimePhasesSyntax

import scala.language.higherKinds

/**
  * Base trait. Used for statefully processing Event's. In each step returns some PhaseResult and new State
  *
  * @tparam Event - events to process
  * @tparam State - inner state
  * @tparam T     - output type, used if phase successfully terminated
  */
trait PhaseParser[Event, State, +T] extends ((Event, State) => (PhaseResult[T], State)) {
  def initialState: State

  def aggregate(event: Event, state: State): State = apply(event, state)._2
}

object PhaseParser {

  trait WithParser[Event, State, T] {
    val parser: PhaseParser[Event, State, T]
  }

  implicit class PhaseParserRich[Event, State, T](val parser: PhaseParser[Event, State, T])
    extends WithParser[Event, State, T]
      with TimePhasesSyntax[Event, State, T]
      with BooleanPhasesSyntax[Event, State, T]
      with NumericPhasesSyntax[Event, State, T]
      with CombiningPhasesSyntax[Event, State, T]
      with MonadPhasesSyntax[Event, State, T]

}