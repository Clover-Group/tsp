package ru.itclover.tsp.nfa
import cats.data.State
import cats.{Id, Monad, Semigroup}
import ru.itclover.tsp.core.{Time, Window}
import cats.implicits._

import scala.collection.immutable.Queue
import scala.language.higherKinds

trait Creatable[-From, To] {
  def create(t: From): To
}

object Creatable {
  type UnitCreatable[T] = Creatable[Unit, T]

  @inline def instance[From, To](f: From => To): Creatable[From, To] = new Creatable[From, To] {
    override def create(x: From): To = f(x)
  }

  @inline def instance[From, To](x: To): Creatable[From, To] = new Creatable[From, To] {
    override def create(t: From): To = x
  }
}

sealed trait Pattern[Event, State, T, F[_]] extends ((State, Event) => F[(State, Option[T])]) with Serializable {}

case class AndThenPattern[Event, State1, State2, T1, T2, F[_]: Monad](
  initialState: State1,
  first: Pattern[Event, State1, T1, F],
  second: Pattern[Event, State2, T2, F]
)(
  implicit creatable: Creatable[T1, State2],
  semigroup: Semigroup[State2]
) extends Pattern[Event, AndThenState[State1, State2], T2, F] {

  def apply(oldState: AndThenState[State1, State2], event: Event): F[(AndThenState[State1, State2], Option[T2])] = {

    val firstF = first.apply(oldState.first, event)
    val secondF = second.apply(oldState.second, event)

    for (firstResult <- firstF;
         (newFirst, resultFirst) = firstResult;
         secondResult <- secondF;
         (newSecond, resultSecond) = secondResult)
      yield AndThenState(newFirst, resultFirst.fold(newSecond)(t => creatable.create(t) |+| newSecond)) -> resultSecond
  }
}

case class AndThenState[State1, State2](first: State1, second: State2)

object AndThenState {}

class QueuePattern[Event, InnerState, T1, T2, F[_]: Monad](
  inner: Pattern[Event, InnerState, T1, F],
  cond: Queue[T1] => (Queue[T1], Option[T2])
) extends Pattern[Event, QueueState[InnerState, T1], T2, F] {
  override def apply(
    oldState: QueueState[InnerState, T1],
    event: Event
  ): F[(QueueState[InnerState, T1], Option[T2])] = {
    val innerF = inner(oldState.innerState, event)
    for (innerUnpacked <- innerF;
         (newInner, innerResult: Option[T1]) = innerUnpacked)
      yield {
        val queue = innerResult.fold(oldState.queue)(t => oldState.queue.enqueue(t))
        val (newQueue, output) = cond(queue)
        QueueState(newInner, newQueue) -> output
      }
  }

}

case class QueueState[InnerState, T](innerState: InnerState, queue: Queue[T])

class CouplePattern[Event, State1, State2, T1, T2, T3, F[_]: Monad](
  left: Pattern[Event, State1, T1, F],
  right: Pattern[Event, State2, T2, F]
)(func: (T1, T2) => T3)
    extends Pattern[Event, CoupleState[State1, State2], T3, F] {
  final override def apply(
    state: CoupleState[State1, State2],
    event: Event
  ): F[(CoupleState[State1, State2], Option[T3])] = {
    val leftF = left.apply(state.left, event)
    val rightF = right.apply(state.right, event)
    for (leftUnpacked  <- leftF;
         rightUnpacked <- rightF;
         (newLeft, leftResult) = leftUnpacked;
         (newRight, rightResult) = rightUnpacked) yield {
      val output = for (_  <- if (state.counter > 0) Some(()) else None;
                        t1 <- leftResult;
                        t2 <- rightResult) yield func(t1, t2)
      CoupleState(state.counter - 1, newLeft, newRight) -> output
    }
  }
}

case class CoupleState[State1, State2](counter: Int, left: State1, right: State2)

case object CoupleState {
  implicit def semigroup[State1: Semigroup, State2: Semigroup]: Semigroup[CoupleState[State1, State2]] =
    Semigroup.instance {
      case (cs1, cs2) => CoupleState(cs1.counter |+| cs2.counter, cs1.left |+| cs2.left, cs1.right |+| cs2.right)
    }

  import Creatable._
  implicit def creatable[T, State1: UnitCreatable, State2: UnitCreatable]: UnitCreatable[CoupleState[State1, State2]] =
    Creatable.instance(
      CoupleState(
        counter = 1,
        implicitly[UnitCreatable[State1]].create(()),
        implicitly[UnitCreatable[State2]].create(())
      )
    )
}

case class DoublePattern[Event, F[_]: Monad](f: Event => Double) extends Pattern[Event, NoState, Double, F] {
  override def apply(noState: NoState, e: Event): F[(NoState, Option[Double])] = Monad[F].point(noState -> Option(f(e)))
}

case class NoState private () extends Serializable

object NoState {
  val instance = new NoState()

  implicit val semigroup: Semigroup[NoState] = Semigroup.instance[NoState] { case (_, _) => instance }
  implicit def creatable[From]: Creatable[From, NoState] = Creatable.instance(NoState.instance)
}

class StateMachine {

  def run[Event, Output](events: Iterable[Event]): Iterable[Output] = {

    type F[A] = Id[A]
    type IdPattern[State] = Pattern[Event, State, Output, F]

    type State = Double

    val p: IdPattern[State] = ???

    val creator: Creatable[Unit, State] = ???
    implicit val semigroup: Semigroup[State] = ???

    val initialState = creator.create(())

    val states: Iterable[(State, Option[Output])] =
      Iterable(initialState -> None)
    events.zip(states).map {
      case (event, (state, _)) => {
        val newState = initialState |+| state
        p.apply(newState, event)
      }
    }

    states.flatMap(_._2)
  }

}
