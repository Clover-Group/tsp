package ru.itclover.streammachine

import ru.itclover.streammachine.core.PhaseResult._
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult}

import scala.collection.mutable

case class StateMachineMapper[Event, State, Out](phaseParser: PhaseParser[Event, State, Out])
  extends AbstractStateMachineMapper[Event, State, Out] {

  private var states: Seq[(PhaseResult[Out], State)] = Vector.empty

  private val collector = mutable.ListBuffer.empty[TerminalResult[Out]]

  def apply(t: Event): this.type = {
    val (firstTerminal, newStates) = process(t, states)
    //todo emit only one Failure in a row
    firstTerminal.foreach(x => collector.append(x))

    states = newStates

    this
  }

  def result: Vector[TerminalResult[Out]] = collector.toVector
}
