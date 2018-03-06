package ru.itclover.streammachine


import java.time.Instant

import com.typesafe.scalalogging.Logger
import ru.itclover.streammachine.aggregators.AggregatorPhases.Segment
import ru.itclover.streammachine.core.Time.timeOrdering
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult, Time}
import ru.itclover.streammachine.core.PhaseResult.{TerminalResult, _}
import ru.itclover.streammachine.core.Time.TimeExtractor


trait AbstractStateMachineMapper[Event, State, Out] {

  private val log = Logger("AbstractStateMachineMapper")

  /** Do apply state machine mapper to old state with that event?
    * Useful to check that there is not significant gap between this and previous event */
  def doProcessOldState(event: Event): Boolean = true

  /** Is it last event in a stream? */
  def isEventTerminal(event: Event): Boolean = false

  def phaseParser: PhaseParser[Event, State, Out]

  def process(event: Event, oldStates: Seq[State]): (Seq[TerminalResult[Out]], Seq[State]) = {
    log.debug(s"Search for patterns in: $event")
    if (isEventTerminal(event)) {
      log.info("Shut down old states, terminator received")
      Seq(Failure("Terminator received")) -> Seq.empty
    } else {
      val doOldStates = doProcessOldState(event)
      // new state is adding every time to account every possible outcomes considering terminal nature of phase mappers
      val oldStatesWithOneInitialState = if (doOldStates) {
        oldStates :+ phaseParser.initialState
      } else {
        log.info("Drop old states, doProcessOldState = false")
        Seq(phaseParser.initialState)
      }

      val stateToResult = phaseParser.curried(event)

      val resultsAndStates = oldStatesWithOneInitialState.map(stateToResult)

      val (toEmit, newStates) = resultsAndStates.span(_._1.isTerminal)

      val emitsLog = toEmit.map(resAndSt => phaseParser.format(event, resAndSt._2) + " emits " + resAndSt._1)
      if (emitsLog.nonEmpty) log.debug(s"Results to emit: ${emitsLog.mkString("\n", "\n", "")}")
      log.debug(s"States on hold: ${newStates.size}")

      toEmit.map(_._1).asInstanceOf[Seq[TerminalResult[Out]]] -> newStates.map(_._2)
    }
  }

}

