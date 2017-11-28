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
        Stay -> None
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
        Stay -> None
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
    * Timer parser. Returns:
    * Stay - if passed less than atLeastSeconds
    * Success - if passed time is between atLeastSeconds and atMaxSeconds
    * Failure - if passed more than atMaxSeconds.
    *
    * @param extract
    * @param atLeastSeconds
    * @param atMaxSeconds
    * @tparam Event - events to process
    */
  case class Timer[Event](extract: Event => Instant, atLeastSeconds: Int = 0, atMaxSeconds: Int = Int.MaxValue)
    extends PhaseParser[Event, Option[Instant], (Instant, Instant)] {

    assert(atLeastSeconds >= 0 && atMaxSeconds >= 0 && atMaxSeconds > atLeastSeconds,
      s"Incorrect Timer configuration (atLeastSeconds: $atLeastSeconds, atMaxSeconds: $atMaxSeconds)")

    override def apply(event: Event, state: Option[Instant]): (PhaseResult[(Instant, Instant)], Option[Instant]) = {

      val eventTime = extract(event)

      val eventMilliSeconds = eventTime.toEpochMilli

      state match {
        case None =>
          Stay -> Some(eventTime)
        case Some(startTime) =>
          val oldMilliSeconds = startTime.toEpochMilli
          (eventMilliSeconds - oldMilliSeconds match {
            case x if x < atLeastSeconds.toLong * 1000l => Stay
            case x if x >= atLeastSeconds.toLong * 1000l && x <= atMaxSeconds.toLong * 1000l => Success(startTime -> eventTime)
            case x if x > atMaxSeconds.toLong * 1000l => Failure(s"Timeout expired at $eventTime")
            case x => Failure("Illegal time!")
          }) -> state
      }
    }

    override def initialState: Option[Instant] = None
  }


  /**
    * Phase waiting for changes of `extract(event)`. Returns Stay if `extract(event)` is the same for subsequence of events.
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

}
