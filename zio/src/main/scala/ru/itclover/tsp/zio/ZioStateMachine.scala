package ru.itclover.tsp.zio
import ru.itclover.tsp.v2.{PState, Pattern, StateMachine}
import scalaz.zio.{UIO, ZIO}
import scalaz.zio.interop.catz._

class ZioStateMachine[Event] {

  val stateMachine = StateMachine[UIO]

  def run[Event, State<: PState[T, State], T](pattern: Pattern[Event, State, T])(input: ZIO[_, _, Event]): Unit ={
    ???
  }
}
