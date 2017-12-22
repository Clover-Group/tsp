package ru.itclover.streammachine.core

import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}

object ConstantPhaseParser {

  def apply[Event, T](value: T): PhaseParser[Event, Unit, T] = new OneRowPhaseParser[Event, T] {
    override def extract(event: Event): T = value
  }
}

trait OneRowPhaseParser[Event, +T] extends PhaseParser[Event, Unit, T] {

  override def apply(v1: Event, v2: Unit): (PhaseResult[T], Unit) = Success(extract(v1), Map.empty) -> ()

  def extract(event: Event): T

  override def initialState = ()
}

object OneRowPhaseParser {
  def apply[Event, T](f: Event => T): OneRowPhaseParser[Event, T] = new OneRowPhaseParser[Event, T]() {
    override def extract(event: Event) = f(event)
  }
}

case class FailurePhaseParser[Event](msg: String) extends OneRowPhaseParser[Event, Nothing] {
  override def apply(v1: Event, v2: Unit) = Failure(msg) -> ()

  override def extract(event: Event) = ???
}