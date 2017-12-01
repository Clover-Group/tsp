package ru.itclover.streammachine


import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Terminate}
import ru.itclover.streammachine.core.PhaseResult._

trait AbstractStateMachineMapper[Event, State, Out] {

  def phaseParser: PhaseParser[Event, State, Out]

  //we terminate our parser. So if it got to the Terminal state if will be stay there forever.
  def terminatedParser = Terminate(phaseParser)

  def process(t: Event, oldStates: Seq[(PhaseResult[Out], State)]): (Option[TerminalResult[Out]], Seq[(PhaseResult[Out], State)]) = {

    val oldStatesWithOneInitialState = oldStates :+ terminatedParser.initialState

    val stateToResult = terminatedParser.curried(t)

    val (firstTerminal, newStates) = oldStatesWithOneInitialState
      .foldLeft((Option.empty[TerminalResult[Out]], Vector.empty[(PhaseResult[Out], State)])) {
        case ((first, coll), oldState) =>

          val (res, newState) = stateToResult(oldState)

          val newFirst = res match {
            case s: TerminalResult[Out] if first.isEmpty => Some(s)
            case _ => first
          }

          val newColl = res match {
            case Failure(_) => coll
            case Success(_) if first.nonEmpty => coll // no more than one event emitted for one input.
            case x => coll :+ newState
          }

          newFirst -> newColl
      }

    firstTerminal -> newStates
  }

}