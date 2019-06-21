package ru.itclover.tsp.zio
import ru.itclover.tsp.v2.{IdxValue, PState, Pattern, StateMachine}
import scalaz.zio.{Queue, UIO, ZIO}
import scalaz.zio.interop.catz._
import scala.language.reflectiveCalls

object ZioStateMachine {

  val stateMachine = StateMachine[UIO]

  def run[Event, State <: PState[T, State], T](
    pattern: Pattern[Event, State, T]
  )(input: UIO[Iterable[Event]]): UIO[Queue[IdxValue[T]]] = {

    for (queue   <- Queue.unbounded[IdxValue[T]];
         inputIt <- input;
         _ <- stateMachine.run(
           pattern,
           inputIt,
           pattern.initialState(),
           (out: IdxValue[T]) => queue.offer(out) *> UIO.apply(())
         ))
      yield queue
  }
}
