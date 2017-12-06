package ru.itclover.streammachine.phases

import java.time.Instant

import ru.itclover.streammachine.core.PhaseResult._
import ru.itclover.streammachine.core._
import Ordered._

object Phases {

  /**
    * PhaseParser to check some condition on any event.
    *
    * @param predicate
    * @tparam Event - events to process
    */
  case class Assert[Event](predicate: Event => Boolean) extends PhaseParser[Event, Unit, Boolean] {
    override def apply(event: Event, s: Unit) = {

      if (predicate(event)) Success(true) -> ()
      else Failure("Event does not match condition!") -> ()
    }

    override def initialState: Unit = ()
  }

  /**
    * Expect decreasing of value. If value has entered to range between from and to, it must monotonically decrease to `to` for success.
    *
    * @param extract - function to extract value to compare
    * @param from    - value to start. if field is bigger than from, result is Stay
    * @param to      - value to stop.
    * @tparam Event - events to process
    */
  case class Decreasing[Event, T: Ordering](extract: Event => T, from: T, to: T)
    extends PhaseParser[Event, Option[T], T] {

    override def apply(event: Event, oldValue: Option[T]): (PhaseResult[T], Option[T]) = {
      val newValue = extract(event)

      def processNewValue = if (newValue > from) {
        Failure("Not in range") -> None
      } else if (newValue <= to) {
        Success(newValue) -> Some(newValue)
      } else {
        Stay -> Some(newValue)
      }

      oldValue match {
        case None => processNewValue
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
  case class Increasing[Event, T: Ordering](extract: Event => T, from: T, to: T) extends PhaseParser[Event, Option[T], T] {

    override def apply(event: Event, oldValue: Option[T]): (PhaseResult[T], Option[T]) = {
      val newValue = extract(event)

      def processNewValue = if (newValue < from) {
        Failure("Not in range") -> None
      } else if (newValue >= to) {
        Success(newValue) -> Some(newValue)
      } else {
        Stay -> Some(newValue)
      }

      oldValue match {
        case None => processNewValue
        case Some(value) =>
          if (newValue < value) {
            Failure("It does not increase") -> oldValue
          } else processNewValue
      }
    }

    override def initialState: Option[T] = None
  }


  /**
    * Phase checking that extract(event) is the same.
    *
    * @param extract
    * @tparam Event
    */
  case class Constant[Event, T](extract: Event => T) extends PhaseParser[Event, Option[T], T] {
    override def apply(event: Event, state: Option[T]): (PhaseResult[T], Option[T]) = {

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
    * Phase waiting for changes of `extract(event)`. Returns Stay if `extract(event)` is the same for subsequence of events.
    *
    * @param extract - function to extract value from Event
    */
  case class Changed[Event, T](extract: Event => T) extends PhaseParser[Event, Option[Set[T]], Set[T]] {
    override def apply(event: Event, state: Option[Set[T]]): (PhaseResult[Set[T]], Option[Set[T]]) = {

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

  case class Wait[Event, State](conditionParser: PhaseParser[Event, State, Boolean]) extends PhaseParser[Event, State, Boolean] {

    override def apply(event: Event, v2: State): (PhaseResult[Boolean], State) = {

      val (res, newState) = conditionParser(event, v2)

      (res match {
        case s@Success(true) => s
        case _ => Stay
      }) -> newState
    }

    override def initialState = conditionParser.initialState
  }

}
