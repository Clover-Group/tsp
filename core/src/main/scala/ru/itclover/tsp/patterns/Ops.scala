package ru.itclover.tsp.patterns

import java.time.Instant
import ru.itclover.tsp.core.PatternResult._
import ru.itclover.tsp.core._
import ru.itclover.tsp.io.{Decoder, Extractor}
import ru.itclover.tsp.patterns.Constants.ExtractingPattern
import ru.itclover.tsp.patterns.Numerics.NumericPhaseParser

object Ops {

  /**
    * Convenient implicit for using Symbols as it event columns. For test purposes only, for production use DSL.
    * @param id - name of the field to extract from event
    * @tparam EItem - type of event elements
    */
  implicit class SymbolToPattern[Event, EItem](val id: Symbol) extends AnyVal with Serializable {
    def asDouble(
      implicit e: Extractor[Event, Symbol, EItem],
      d: Decoder[EItem, Double]
    ): NumericPhaseParser[Event, NoState] = ExtractingPattern(id, id)

    def as[T](implicit e: Extractor[Event, Symbol, EItem], d: Decoder[EItem, T]) = ExtractingPattern(id, id)
  }

  /**
    * Phase terminating inner parser. If inner parser at least once got to the TerminalResult it will stay there forever
    *
    * @param inner - parser to be terminated
    * @tparam Event - events to process
    * @tparam State - inner state
    */
  case class Terminate[Event, State, Out](inner: Pattern[Event, State, Out]) extends Pattern[Event, (PatternResult[Out], State), Out] {

    override def apply(event: Event, v2: (PatternResult[Out], State)): (PatternResult[Out], (PatternResult[Out], State)) = {
      val (phaseResult, state) = v2

      phaseResult match {
        case x: TerminalResult[Out] => x -> v2
        case Stay =>
          val (nextResult, nextState) = inner.apply(event, state)
          nextResult -> (nextResult, nextState)
      }
    }

    override def aggregate(v1: Event, v2: (PatternResult[Out], State)) = Stay -> inner.aggregate(v1, v2._2)

    override def initialState = Stay -> inner.initialState
  }

    /**
    * Phase checking that extract(event) is the same (not changing).
    */
  case class ConstantCheck[Event, T](extract: Event => T) extends Pattern[Event, Option[T], T] {
    override def apply(event: Event, state: Option[T]): (PatternResult[T], Option[T]) = {

      val field = extract(event)

      state match {
        case Some(old) if old == field => Success(field) -> state
        case Some(old) => Failure("Field has changed!") -> state
        case None => Stay -> Some(field)
      }
    }

    override def initialState: Option[T] = None
  }


  /**
    * Phase waiting for changes of `extract(event)`.
    * Returns Stay if `extract(event)` is the same for subsequence of events.
    *
    * @param extract - function to extract value from Event
    */
  case class ChangeCheck[Event, T](extract: Event => T) extends Pattern[Event, Option[Set[T]], Set[T]] {

    override def apply(event: Event, state: Option[Set[T]]): (PatternResult[Set[T]], Option[Set[T]]) = {

      val newValue = extract(event)

      val diffValues = state.map(_ + newValue).getOrElse(Set(newValue))

      val newState = Some(diffValues)
      if (diffValues.size == 1) {
        Stay -> newState
      } else {
        Success(diffValues) -> newState
      }
    }

    override def initialState: Option[Set[T]] = None
  }
}
