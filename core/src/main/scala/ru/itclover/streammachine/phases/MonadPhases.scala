package ru.itclover.streammachine.phases

import ru.itclover.streammachine.core.PhaseParser.WithParser
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult}

object MonadPhases {

  trait MonadPhasesSyntax[Event, State, T] {
    this: WithParser[Event, State, T] =>

    def mapWithEvent[B](f: (Event, T) => B): MapWithEventParser[Event, State, T, B] = MapWithEventParser(parser, f.curried)

    def map[B](f: T => B): MapParser[Event, State, T, B] = MapParser(parser)(f)

    def flatMap[State2, Out2](f: T => PhaseParser[Event, State2, Out2]) = FlatMapParser(parser, f)
  }

  case class MapWithEventParser[Event, State, In, Out](phaseParser: PhaseParser[Event, State, In], f: Event => In => Out) extends PhaseParser[Event, State, Out] {

    override def apply(event: Event, oldState: State): (PhaseResult[Out], State) = {
      val (phaseResult, state) = phaseParser.apply(event, oldState)
      (phaseResult.map(f(event)), state)
    }

    override def aggregate(event: Event, oldState: State): State = phaseParser.aggregate(event, oldState)

    override def initialState = phaseParser.initialState
  }


  abstract class MapParserLike[Event, State, In, Out](phaseParser: PhaseParser[Event, State, In])(f: In => Out) extends PhaseParser[Event, State, Out] {

    override def apply(event: Event, oldState: State): (PhaseResult[Out], State) = {
      val (phaseResult, state) = phaseParser.apply(event, oldState)
      (phaseResult.map(f), state)
    }

    override def format(event: Event, state: State): String = s"MapParser( ${phaseParser.format(event, state)} mapped with $f)"

    override def aggregate(event: Event, oldState: State): State = phaseParser.aggregate(event, oldState)

    override def initialState = phaseParser.initialState
  }

  case class MapParser[Event, State, In, Out](phaseParser: PhaseParser[Event, State, In])(f: In => Out)
    extends MapParserLike(phaseParser)(f) {
    val function: In => Out = f
    type InType = In
    type OutType = Out
  }

  case class FlatMapParser[Event, State1, State2, Out1, Out2](parser: PhaseParser[Event, State1, Out1], f: Out1 => PhaseParser[Event, State2, Out2]) extends PhaseParser[Event, Either[State1, (PhaseParser[Event, State2, Out2], State2)], Out2] {

    // TODO: Either => Tuple
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

    // todo do not use aggregation with flatmap parsers
    override def aggregate(event: Event, state: Either[State1, (PhaseParser[Event, State2, Out2], State2)]): Either[State1, (PhaseParser[Event, State2, Out2], State2)] = ???

    override def initialState = Left(parser.initialState)
  }

}
