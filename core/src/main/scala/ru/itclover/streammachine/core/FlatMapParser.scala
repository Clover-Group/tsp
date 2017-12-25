package ru.itclover.streammachine.core

import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}

case class FlatMapParser[Event, State1, State2, Out1, Out2](parser: PhaseParser[Event, State1, Out1], f: Out1 => PhaseParser[Event, State2, Out2]) extends PhaseParser[Event, Either[State1, (PhaseParser[Event, State2, Out2], State2)], Out2] {

  override def apply(event: Event, state: (Either[State1, (PhaseParser[Event, State2, Out2], State2)])): (PhaseResult[Out2], (Either[State1, (PhaseParser[Event, State2, Out2], State2)])) = {

    state match {
      case Left(firstState) =>
        val (firstResult, newFirstState) = parser(event, firstState)
        firstResult match {
          case Success(firstOut) => {
            //we should try to reapply this event to the second phase
            val nextParser = f(firstOut)
            val (secondResult, newSecondState) = nextParser(event, nextParser.initialState)
            val newState = Right(nextParser, newSecondState)
            (secondResult match {
              case Success(secondOut) => Success(secondOut)
              case f@Failure(msg) => f
              case Stay => Stay
            }) -> newState
          }
          case f@Failure(msg) => f -> Left(newFirstState)
          case Stay => Stay -> Left(newFirstState)
        }
      case Right((secondParser, secondState)) =>
        val (secondResult, newSecondState) = secondParser(event, secondState)
        secondResult -> Right(secondParser, newSecondState)
    }
  }

  override def initialState = Left(parser.initialState)
}