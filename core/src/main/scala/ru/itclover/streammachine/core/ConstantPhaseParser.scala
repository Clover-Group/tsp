package ru.itclover.streammachine.core

abstract class TrivialPhaseParser[T](v: T) extends  {

}

abstract class OneRowPhaseParser[Event, T]extends PhaseParser[Event, Unit, T]