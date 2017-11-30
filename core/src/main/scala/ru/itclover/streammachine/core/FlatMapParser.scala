package ru.itclover.streammachine.core

case class FlatMapParser[Event, State, State2, Out, Out2](parser: PhaseParser[Event, State, Out], f: Out => PhaseParser[Event, State2, Out2]) extends PhaseParser[Event, State2, Out2] {

  override def apply(event: Event, state: State2) = {
    ???
  }

  override def initialState = ???
}


case class Any()


object FlatMapParser {


}
