package ru.itclover.streammachine


import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Terminate}
import ru.itclover.streammachine.core.PhaseResult._

trait AbstractStateMachineMapper[Event, State, Out] {

  def phaseParser: PhaseParser[Event, State, Out]

  //we terminate our parser. So if it got to the Terminal state if will be stay there forever.
  def terminatedParser = Terminate(phaseParser)

  def process(t: Event, oldStates: Seq[State]): (Seq[TerminalResult[Out]], Seq[ State]) = {

    val oldStatesWithOneInitialState = oldStates :+ phaseParser.initialState

    val stateToResult = phaseParser.curried(t)

    val (toEmit, newStates) = oldStatesWithOneInitialState.map(stateToResult).partition(_._1.isTerminal)

    toEmit.map(_._1).asInstanceOf[Seq[TerminalResult[Out]]] -> newStates.map(_._2)
  }

}