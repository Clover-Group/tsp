package ru.itclover.streammachine.phases

import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult}

object ConstantPhases {

  def apply[Event, T](value: T): PhaseParser[Event, Unit, T] = new OneRowPhaseParser[Event, T] {
    override def extract(event: Event): T = value
  }

  trait OneRowPhaseParser[Event, +T] extends PhaseParser[Event, Unit, T] {

    override def apply(v1: Event, v2: Unit): (PhaseResult[T], Unit) = Success(extract(v1)) -> ()

    override def aggregate(event: Event, state: Unit): Unit = initialState

    def extract(event: Event): T

    override def initialState: Unit = ()
  }

  object OneRowPhaseParser {
    def apply[Event, T](f: Event => T): OneRowPhaseParser[Event, T] = new OneRowPhaseParser[Event, T]() {
      override def extract(event: Event) = f(event)
    }
  }

  case class FailurePhaseParser[Event](msg: String) extends OneRowPhaseParser[Event, Nothing] {
    override def apply(v1: Event, v2: Unit): (Failure, Unit) = Failure(msg) -> ()

    override def extract(event: Event): Nothing = ???
  }

}
