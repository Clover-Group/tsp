package ru.itclover.streammachine

import ru.itclover.streammachine.core.PhaseResult._
import ru.itclover.streammachine.core.{InitialState, PhaseParser}

import scala.collection.mutable

case class StateMachineMapper[Event, State: InitialState, Out](phaseParser: PhaseParser[Event, State, Out]) {

  private val initial: State = implicitly[InitialState[State]].initial

  private var state: State = initial

  private val collector = mutable.ListBuffer.empty[Out]

  def apply(t: Event): this.type = {
    val (result, newState) = phaseParser.apply(t, state)

    state = newState

    result match {
      case Stay => println(s"stay for event $t")
      case Failure(msg) =>
        //todo Should we try to run this message again?
        println(msg)
        state = initial
      case Success(out) =>
        collector += out
    }
    this
  }

  def result: Seq[Out] = collector.toVector
}