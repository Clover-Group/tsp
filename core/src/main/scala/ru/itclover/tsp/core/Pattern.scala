package ru.itclover.tsp.core

import ru.itclover.tsp.aggregators.AggregatorPhases.AggregatorFunctions
import ru.itclover.tsp.phases.BooleanPhases.{BooleanFunctions, BooleanPatternsSyntax}
import ru.itclover.tsp.phases.CombiningPhases.CombiningPatternsSyntax
import ru.itclover.tsp.phases.ConstantPhases.ConstantFunctions
import ru.itclover.tsp.phases.MonadPhases.MonadPatternsSyntax
import ru.itclover.tsp.phases.NumericPhases.{NumericFunctions, NumericPatternsSyntax}
import ru.itclover.tsp.phases.TimePhases.TimePatternsSyntax

import scala.language.higherKinds


/**
  * Base trait. Used for statefully processing Event's. In each step returns some PatternResult and new State
  *
  * @tparam Event - events to process
  * @tparam State - inner state
  * @tparam T     - output type, used if phase successfully terminated
  */
trait Pattern[Event, State, +T] extends ((Event, State) => (PatternResult[T], State)) with Serializable {
  def initialState: State

  def aggregate(event: Event, state: State): State = apply(event, state)._2

  /**
    * @return the best possible string representation of rule with state account and inserted event values
    */
  def format(event: Event, state: State): String = s"${this.getClass.getSimpleName}($state)"

  /**
    * @return the best possible string representation of rule with __initialState__ and inserted event values
    */
  def format(event: Event): String = format(event, initialState)
}

object Pattern {

  trait WithPattern[Event, State, T] {
    val parser: Pattern[Event, State, T]
  }

  implicit class PatternRich[Event, State, T](val parser: Pattern[Event, State, T])
    extends WithPattern[Event, State, T]
      with TimePatternsSyntax[Event, State, T]
      with BooleanPatternsSyntax[Event, State, T]
      with NumericPatternsSyntax[Event, State, T]
      with CombiningPatternsSyntax[Event, State, T]
      with MonadPatternsSyntax[Event, State, T]

  object Functions
    extends AggregatorFunctions
      with BooleanFunctions
      with ConstantFunctions
      with NumericFunctions

}