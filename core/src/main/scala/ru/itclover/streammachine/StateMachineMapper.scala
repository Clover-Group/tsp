package ru.itclover.streammachine

import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.core.PhaseResult._

import scala.collection.mutable

case class StateMachineMapper[Event, State, Out](phaseParser: PhaseParser[Event, State, Out]) {

  private var state: State = phaseParser.initialState

  private val collector = mutable.ListBuffer.empty[Out]

  def apply(t: Event): this.type = {
    val (result, newState) = phaseParser.apply(t, state)

    state = newState

    result match {
      case Stay => // println(s"stay for event $t")
      case Failure(msg) =>
        //todo Should we try to run this message again?
        //        println(msg)
        state = phaseParser.initialState
      case Success(out) =>
        collector += out
    }
    this
  }

  def result: Seq[Out] = collector.toVector
}