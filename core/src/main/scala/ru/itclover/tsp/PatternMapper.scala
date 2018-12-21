package ru.itclover.tsp

import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.core.PatternResult._
import ru.itclover.tsp.core.Pattern

import scala.collection.mutable

case class PatternMapper[Event, State, PhaseOut, MapperOut]
  (pattern: Pattern[Event, State, PhaseOut], mapResults: ResultMapper[Event, PhaseOut, MapperOut])
  extends AbstractPatternMapper[Event, State, PhaseOut]
{
  val log: Logger = Logger[PatternMapper[Event, State, PhaseOut, MapperOut]]

  private var states: Seq[State] = Vector.empty

  private val collector = mutable.ListBuffer.empty[TerminalResult[MapperOut]]


  def apply(event: Event): this.type = {
    val (results, newStates) = process(event, states)

    mapResults(event, results).foreach(x => collector.append(x))

    states = newStates

    this
  }

  def result: Vector[TerminalResult[MapperOut]] = collector.toVector

  override def isEventTerminal(event: Event) = false // TODO
}
