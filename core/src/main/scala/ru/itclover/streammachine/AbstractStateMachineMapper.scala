package ru.itclover.streammachine


import java.time.Instant
import ru.itclover.streammachine.core.Aggregators.Segment
import ru.itclover.streammachine.core.Time.timeOrdering
import ru.itclover.streammachine.core.PhaseParser.And
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Terminate, Time}
import ru.itclover.streammachine.core.PhaseResult.{TerminalResult, _}
import ru.itclover.streammachine.core.Time.TimeExtractor


trait AbstractStateMachineMapper[Event, State, Out] {

  def phaseParser: PhaseParser[Event, State, Out]

  def process(event: Event, oldStates: Seq[State]): (Seq[TerminalResult[Out]], Seq[State]) = {

    // new state is adding every time to account every possible outcomes considering terminal nature of phase mappers
    val oldStatesWithOneInitialState = oldStates :+ phaseParser.initialState

    val stateToResult = phaseParser.curried(event)

    val resultsAndStates = oldStatesWithOneInitialState.map(stateToResult)

    val (toEmit, newStates) = resultsAndStates.span(_._1.isTerminal)

    toEmit.map(_._1).asInstanceOf[Seq[TerminalResult[Out]]] -> newStates.map(_._2)
  }

}