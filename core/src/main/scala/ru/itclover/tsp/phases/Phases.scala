package ru.itclover.tsp.phases

import java.time.Instant

import ru.itclover.tsp.core.PatternResult._
import ru.itclover.tsp.core._
import Ordered._

object Phases {

  type Phase[Event] = Pattern[Event, _, _]

  trait AnyExtractor[Event] extends ((Event, Symbol) => Any) with Serializable
  trait AnyNonTransformedExtractor[Event] extends ((Event, Symbol) => Any) with Serializable

  // TODO Rm
  /**
    * Expect decreasing of value. If value has entered to range between from and to, it must monotonically decrease to `to` for success.
    *
    * @param extract - function to extract value to compare
    * @param from    - value to start. if field is bigger than from, result is Stay
    * @param to      - value to stop.
    * @tparam Event - events to process
    */
  case class Decreasing[Event, T: Ordering](extract: Event => T, from: T, to: T)
    extends Pattern[Event, Option[T], T] {

    override def apply(event: Event, oldValue: Option[T]): (PatternResult[T], Option[T]) = {
      val newValue = extract(event)

      def processNewValue = if (newValue > from) {
        Failure("Not in range") -> None
      } else if (newValue <= to) {
        Success(newValue) -> Some(newValue)
      } else {
        Stay -> Some(newValue)
      }

      oldValue match {
        case None => {
          if (from == newValue) {
            if (to == from) Success(newValue) -> Some(newValue)
            else Stay -> Some(newValue)
          }
          else Failure(s"Not hit from($from) value before decrease.") -> None
        }
        case Some(value) => {
          if (newValue > value) {
            Failure("It does not decrease") -> oldValue
          } else processNewValue
        }
      }
    }

    override def initialState: Option[T] = None
  }

  /**
    * Expect increasing of value. If value has entered to range between from and to, it must monotonically increase to `to` for success.
    *
    * @param extract - function to extract value to compare
    * @param from    - value to start. if field is bigger than from, result is Stay
    * @param to      - value to stop.
    * @tparam Event - events to process
    */
  case class Increasing[Event, T: Ordering](extract: Event => T, from: T, to: T) extends Pattern[Event, Option[T], T] {

    override def apply(event: Event, oldValue: Option[T]): (PatternResult[T], Option[T]) = {
      val newValue = extract(event)

      def processNewValue = if (newValue < from) {
        Failure("Not in range") -> None
      } else if (newValue >= to) {
        Success(newValue) -> Some(newValue)
      } else {
        Stay -> Some(newValue)
      }

      oldValue match {
        case None => {
          if (from == newValue) {
            if (to == from) Success(newValue) -> Some(newValue)
            else Stay -> Some(newValue)
          }
          else Failure(s"Not hit from($from) value before increase.") -> None
        }
        case Some(value) =>
          if (newValue < value) {
            Failure("It does not increase") -> oldValue
          } else processNewValue
      }
    }

    override def initialState: Option[T] = None
  }


  /**
    * Phase checking that extract(event) is the same (not changing).
    *
    * @param extract
    * @tparam Event
    */
  case class Constant[Event, T](extract: Event => T) extends Pattern[Event, Option[T], T] {
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
  case class Changed[Event, T](extract: Event => T) extends Pattern[Event, Option[Set[T]], Set[T]] {
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


  /**
    * Phase terminating inner parser. If inner parser at least once got to the TerminalResult it will stay there forever
    *
    * @param inner - parser to be terminated
    * @tparam Event - events to process
    * @tparam State - inner state
    * @tparam Out
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


}
