package ru.itclover.streammachine.core

import ru.itclover.streammachine.core.PhaseParser.{And, AndThen, Or}
import ru.itclover.streammachine.core.PhaseResult._

/**
  * Base trait. Used for statefully processing Event's. In each step returns some PhaseResult and new State
  *
  * @tparam Event - events to process
  * @tparam State - inner state
  * @tparam T     - output type, used if phase successfully terminated
  */
trait PhaseParser[Event, State, T] extends ((Event, State) => (PhaseResult[T], State)) {
  def initialState: State
}

object PhaseParser {

  /**
    * Type alias to use in AndParser
    *
    * @tparam L
    * @tparam R
    */
  type And[L, R] = (L, R)

  /**
    * Type alias to use in OrParser
    *
    * @tparam L
    * @tparam R
    */
  type Or[L, R] = Either[L, R]
  /**
    * Type alias to use in AndThenParser
    *
    * @tparam L
    * @tparam R
    */
  type AndThen[L, R] = (L, R)

  implicit class PhaseParserRich[Event, State, T](val parser: PhaseParser[Event, State, T]) extends AnyVal {

    /**
      * Allows easily create AndParser like:
      * `parser1 and parser2`
      *
      * @param rightParser - phase parser to add.
      * @return new AndParser
      */
    def and[RightState, RightOut](rightParser: PhaseParser[Event, RightState, RightOut]):
    AndParser[Event, State, RightState, T, RightOut] = AndParser(parser, rightParser)

    /**
      * Alias for `and`
      */
    def &[RightState, RightOut](rightParser: PhaseParser[Event, RightState, RightOut]):
    AndParser[Event, State, RightState, T, RightOut] = and(rightParser)

    /**
      * Allows easily create AndThenParser like:
      * {@code parser1 andThen parser2 }
      *
      * @param nextParser - phase parser to add after this one
      * @return new AndThenParser
      */
    def andThen[NextState, NextOut](nextParser: PhaseParser[Event, NextState, NextOut]):
    AndThenParser[Event, State, NextState, T, NextOut] = AndThenParser(parser, nextParser)

    /**
      * alias for andThen method
      */
    def ~[NextState, NextOut](nextParser: PhaseParser[Event, NextState, NextOut]):
    AndThenParser[Event, State, NextState, T, NextOut] = andThen(nextParser)

    /**
      * Allows easily create OrParser like:
      * `parser1 or parser2`
      *
      * @param rightParser - phase parser to add.
      * @return new OrParser
      */
    def or[RightState, RightOut](rightParser: PhaseParser[Event, RightState, RightOut]):
    OrParser[Event, State, RightState, T, RightOut] = OrParser(parser, rightParser)

    /**
      * Alias for `or`
      */
    def |[RightState, RightOut](rightParser: PhaseParser[Event, RightState, RightOut]):
    OrParser[Event, State, RightState, T, RightOut] = or(rightParser)


    def map[B](f: T => B): PhaseParser[Event, State, B] = new PhaseParser[Event, State, B] {
      override def apply(v1: Event, v2: State): (PhaseResult[B], State) = {
        val (phaseResult, state) = parser.apply(v1, v2)
        (phaseResult.map(f), state)
      }

      override def initialState = parser.initialState
    }
  }

}

/**
  * Parser combining two other parsers.
  * Uses the following rules:
  * Success only if both parts are Success.
  * Failure if any of sides is Failure
  * Else Stay
  */
case class AndParser[Event, LState, RState, LOut, ROut]
(
  leftParser: PhaseParser[Event, LState, LOut],
  rightParser: PhaseParser[Event, RState, ROut]
)
  extends PhaseParser[Event, LState And RState, LOut And ROut] {

  override def apply(event: Event, state: LState And RState): (PhaseResult[LOut And ROut], LState And RState) = {
    val (leftState, rightState) = state

    val (leftResult, newLeftState) = leftParser(event, leftState)
    val (rightResult, newRightState) = rightParser(event, rightState)
    val newState = newLeftState -> newRightState
    ((leftResult, rightResult) match {
      case (Success(leftOut), Success(rightOut)) => Success(leftOut -> rightOut)
      case (Failure(msg), _) => Failure(msg)
      case (_, Failure(msg)) => Failure(msg)
      case (_, _) => Stay
    }) -> newState
  }

  override def initialState = leftParser.initialState -> rightParser.initialState
}

// todo think about using shapeless here.
/**
  * PhaseParser chaining two parsers one after another.
  * Success if first phase finished successfully and second is successfull too.
  * Failure if any of phases finished with Failure.
  * If first phase is in Stay or first phase has finished but second in Stay the result is Stay.
  */
case class AndThenParser[Event, FirstState, SecondState, FirstOut, SecondOut]
(
  first: PhaseParser[Event, FirstState, FirstOut],
  second: PhaseParser[Event, SecondState, SecondOut]
)
  extends PhaseParser[Event, (FirstState, SecondState, Option[FirstOut]), FirstOut AndThen SecondOut] {

  override def apply(event: Event, state: (FirstState, SecondState, Option[FirstOut])):
  (PhaseResult[FirstOut AndThen SecondOut], (FirstState, SecondState, Option[FirstOut])) = {

    // third element in state is the result of first phase. Contains None if first phase was not finished
    val (firstState, secondState, optFirstOut) = state

    optFirstOut match {
      case None =>
        val (firstResult, newFirstState) = first(event, firstState)
        firstResult match {
          case Success(firstOut) => {
            //we should try to reapply this event to the second phase
            val (secondResult, newSecondState) = second(event, secondState)
            val newState = (newFirstState, newSecondState, Some(firstOut))
            (secondResult match {
              case Success(secondOut) => Success(firstOut -> secondOut)
              case f@Failure(msg) => f
              case Stay => Stay
            }) -> newState
          }
          case f@Failure(msg) => f -> (newFirstState, secondState, None)
          case Stay => Stay -> (newFirstState, secondState, None)
        }
      case Some(firstOut) =>
        val (secondResult, newSecondState) = second(event, secondState)

        (secondResult match {
          case Success(secondOut) => Success(firstOut -> secondOut)
          case f@Failure(msg) => f
          case Stay => Stay
        }) -> (firstState, newSecondState, optFirstOut)
    }
  }

  override def initialState = (first.initialState, second.initialState, None)
}

case class OrParser[Event, LState, RState, LOut, ROut]
(
  leftParser: PhaseParser[Event, LState, LOut],
  rightParser: PhaseParser[Event, RState, ROut]
)
  extends PhaseParser[Event, (LState, RState), LOut Or ROut] {

  override def apply(event: Event, state: (LState, RState)): (PhaseResult[LOut Or ROut], (LState, RState)) = {
    val (leftState, rightState) = state

    val (leftResult, newLeftState) = leftParser(event, leftState)
    val (rightResult, newRightState) = rightParser(event, rightState)
    val newState = newLeftState -> newRightState
    ((leftResult, rightResult) match {
      case (Success(leftOut), _) => Success(Left(leftOut))
      case (_, Success(rightOut)) => Success(Right(rightOut))
      case (Stay, _) => Stay
      case (_, Stay) => Stay
      case (Failure(msg1), Failure(msg2)) => Failure(s"Or Failed: 1) $msg1 2) $msg2")
    }) -> newState
  }

  override def initialState = (leftParser.initialState, rightParser.initialState)
}