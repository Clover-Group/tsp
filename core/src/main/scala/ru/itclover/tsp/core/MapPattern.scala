package ru.itclover.tsp.core
import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.PQueue.MapPQueue

case class MapPattern[Event, T1, T2, InnerState](inner: Pattern[Event, InnerState, T1])(val func: T1 => Result[T2])
    extends Pattern[Event, InnerState, T2] {
  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: InnerState,
    oldQueue: PQueue[T2],
    event: Cont[Event]
  ): F[(InnerState, PQueue[T2])] = {

    // we need to trim inner queue here to avoid memory leaks
    val innerQueue = getInnerQueue(oldQueue)
    inner(oldState, innerQueue, event).map {
      case (innerResult, innerQueue) => innerResult -> MapPQueue[T1, T2](innerQueue, _.value.flatMap(func))
    }
  }

  // Using isInstanceOf/asInstanceOf due to type erasure
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.IsInstanceOf"))
  private def getInnerQueue(queue: PQueue[T2]): PQueue[T1] = {
    queue match {
      case MapPQueue(innerQueue, _) if innerQueue.isInstanceOf[PQueue[T1]] => innerQueue.asInstanceOf[PQueue[T1]]
      //this is for first call
      case x if x.size == 0 => x.asInstanceOf[PQueue[T1]]
      // TODO: Dangerous
      case x =>
        x.asInstanceOf[PQueue[T1]] // sys.error("Wrong logic! MapPattern.apply must be called only with MapPQueue")
    }
  }

  override def initialState(): InnerState = inner.initialState()
}
