package ru.itclover.streammachine.core

import ru.itclover.streammachine.core.PhaseParser.{AndThen, And}
import ru.itclover.streammachine.core.PhaseResult._

/**
  * Base trait. Used for statefully processing Event's. In each step returns some PhaseResult and new State
  *
  * @tparam Event - events to process
  * @tparam State - inner state
  * @tparam T     - output type, used if phase successfully terminated
  */
trait PhaseParser[Event, State, T] extends ((Event, State) => (PhaseResult[T], State))

object PhaseParser {

  /**
    * Type alias to use in AndParser
    *
    * @tparam L
    * @tparam R
    */
  type And[L, R] = (L, R)
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
      * @tparam RightState
      * @tparam RightOut
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
      * `parser1 andThen parser2`
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


    def map[B](f: T => B): PhaseParser[Event, State, B] = new PhaseParser[Event, State, B] {
      override def apply(v1: Event, v2: State): (PhaseResult[B], State) = {
        val (phaseResult, state) = parser.apply(v1, v2)
        (phaseResult.map(f), state)
      }
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
}

