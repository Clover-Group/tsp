package ru.itclover.tsp

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.core.PatternResult.{TerminalResult, _}
import com.typesafe.config._

trait AbstractPatternMapper[Event, State, Out] {
  private val isDebug = ConfigFactory.load().getBoolean("general.is-debug")
  private val log = Logger("AbstractPatternMapper")

  private val counter = new AtomicInteger(0)

  /** Is it last event in a stream? */
  def isEventTerminal(event: Event): Boolean = false

  def phaseParser: Pattern[Event, State, Out]

  def process(event: Event, oldStates: Seq[State]): (Seq[TerminalResult[Out]], Seq[State]) = {

    if (counter.longValue() % 1000 == 0) log.debug(s"Search for patterns in: $event")

    if (isEventTerminal(event)) {
      log.info("Shut down old states, terminator received")
      Seq(heartbeat) -> Seq.empty
    } else {
      // new state is adding every time to account every possible outcomes considering terminal nature of phase mappers
      val oldStatesWithOneInitialState = oldStates :+ phaseParser.initialState

      val stateToResult = phaseParser.curried(event)

      val resultsAndStates = oldStatesWithOneInitialState.map(stateToResult)

      val (toEmit, newStates) = resultsAndStates.span(_._1.isTerminal)

      // TODO: Move to SMM or to Writer monad in Phases

      if (isDebug && counter.incrementAndGet() % 1000 == 0) {
        log.debug(s"After ${counter.longValue()} events")
        val emitsLog = toEmit.map(resAndSt => phaseParser.format(event, resAndSt._2) + " emits " + resAndSt._1)
        if (emitsLog.nonEmpty) log.debug(s"Results to emit: ${emitsLog.mkString("\n", "\n", "")}")
        log.debug(s"States on hold: ${newStates.size}")
      }

      toEmit.map(_._1).asInstanceOf[Seq[TerminalResult[Out]]] -> newStates.map(_._2)
    }
  }

}
