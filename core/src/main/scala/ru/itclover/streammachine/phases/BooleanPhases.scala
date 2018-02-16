package ru.itclover.streammachine.phases

import ru.itclover.streammachine.core.PhaseParser.WithParser
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core._
import ru.itclover.streammachine.phases.CombiningPhases.{And, EitherParser, TogetherParser}

object BooleanPhases {

  trait BooleanPhasesSyntax[Event, S, T] {
    this: WithParser[Event, S, T] =>

    def >[S2](right: PhaseParser[Event, S2, T])(implicit ord: Ordering[T]) = GreaterParser(this.parser, right)

    def >=[S2](right: PhaseParser[Event, S2, T])(implicit ord: Ordering[T]) = GreaterOrEqualParser(this.parser, right)

    def <[S2](right: PhaseParser[Event, S2, T])(implicit ord: Ordering[T]) = LessParser(this.parser, right)

    def <=[S2](right: PhaseParser[Event, S2, T])(implicit ord: Ordering[T]) = LessOrEqualParser(this.parser, right)

    def and[S2](right: BooleanPhaseParser[Event, S2])(implicit ev: T =:= Boolean) = AndParser(this.parser.asInstanceOf[BooleanPhaseParser[Event, S]], right)

    def or[S2](right: BooleanPhaseParser[Event, S2])(implicit ev: T =:= Boolean) = OrParser(this.parser.asInstanceOf[BooleanPhaseParser[Event, S]], right)

    /**
      * Alias for `and`
      */
    def &[RightState](rightParser: BooleanPhaseParser[Event, RightState])(implicit ev: T =:= Boolean)  = and(rightParser)


    /**
      * Alias for `or`
      */
    def |[RightState](rightParser: BooleanPhaseParser[Event, RightState])(implicit ev: T =:= Boolean)  = or(rightParser)


    def ===[S2](right: PhaseParser[Event, S2, T]) = EqualParser(this.parser, right)

    def !=[S2](right: PhaseParser[Event, S2, T]) = NonEqualParser(this.parser, right)

  }

  trait BooleanFunctions {
    def not[Event, S](inner: BooleanPhaseParser[Event, S]): NotParser[Event, S] = NotParser(inner)
  }

  type BooleanPhaseParser[Event, State] = PhaseParser[Event, State, Boolean]

  /**
    * PhaseParser to check some condition on any event.
    *
    * @param predicate
    * @tparam Event - events to process
    */
  case class Assert[Event](predicate: Event => Boolean) extends BooleanPhaseParser[Event, Option[Unit]] {
    override def apply(event: Event, s: Option[Unit]) = {

      if (predicate(event)) Success(true) -> None
      else Failure("Event does not match condition.") -> None
    }

    override def initialState: Option[Unit] = None
  }

  /**
    * PhaseParser returning only Success(true), Failure and Stay. Cannot return Success(false)
    *
    * //    * @param condition - inner boolean parser. Resulted parser returns Failure if condition returned Success(true)
    * //    * @tparam Event - event type
    * //    * @tparam State - possible inner state
    */
  abstract class ComparingParser[Event, State1, State2, T]
  (left: PhaseParser[Event, State1, T],
   right: PhaseParser[Event, State2, T])
    extends BooleanPhaseParser[Event, State1 And State2] {

    private val andParser = left togetherWith right

    def compare(a: T, b: T): Boolean

    override def apply(e: Event, state: (State1, State2)): (PhaseResult[Boolean], (State1, State2)) = {
      val (res, newState) = andParser(e, state)

      (res match {
        case Success((a, b)) => Success(compare(a, b))
        case x: Failure => x
        case Stay => Stay
      }) -> newState
    }

    override def aggregate(event: Event, state: (State1, State2)): (State1, State2) = {
      val (leftState, rightState) = state
      left.aggregate(event, leftState) -> right.aggregate(event, rightState)
    }

    override def initialState: (State1, State2) = (left.initialState, right.initialState)
  }


  case class GreaterParser[Event, State1, State2, T](left: PhaseParser[Event, State1, T],
                                                     right: PhaseParser[Event, State2, T])
                                                    (implicit ord: Ordering[T]) extends ComparingParser(left, right) {

    import ord._

    override def compare(a: T, b: T): Boolean = a > b
  }

  case class GreaterOrEqualParser[Event, State1, State2, T](left: PhaseParser[Event, State1, T],
                                                            right: PhaseParser[Event, State2, T])
                                                           (implicit ord: Ordering[T]) extends ComparingParser(left, right) {

    import ord._

    override def compare(a: T, b: T): Boolean = a >= b
  }

  case class LessParser[Event, State1, State2, T](left: PhaseParser[Event, State1, T],
                                                  right: PhaseParser[Event, State2, T])
                                                 (implicit ord: Ordering[T]) extends ComparingParser(left, right) {

    import ord._

    override def compare(a: T, b: T): Boolean = a < b
  }

  case class LessOrEqualParser[Event, State1, State2, T](left: PhaseParser[Event, State1, T],
                                                         right: PhaseParser[Event, State2, T])
                                                        (implicit ord: Ordering[T]) extends ComparingParser(left, right) {

    import ord._

    override def compare(a: T, b: T): Boolean = a <= b
  }

  case class EqualParser[Event, State1, State2, T](left: PhaseParser[Event, State1, T],
                                                   right: PhaseParser[Event, State2, T])
    extends ComparingParser(left, right) {

    override def compare(a: T, b: T): Boolean = a equals b
  }

  case class NonEqualParser[Event, State1, State2, T](left: PhaseParser[Event, State1, T],
                                                      right: PhaseParser[Event, State2, T])
    extends ComparingParser(left, right) {

    override def compare(a: T, b: T): Boolean = !(a equals b)
  }

  case class NotParser[Event, State](inner: BooleanPhaseParser[Event, State])
    extends BooleanPhaseParser[Event, State] {

    override def apply(e: Event, state: State): (PhaseResult[Boolean], State) = {

      val (result, newState) = inner(e, state)
      (result match {
        case Success(b) => Success(!b)
        case x: Failure => x
        case Stay => Stay
      }) -> newState
    }

    override def initialState: State = inner.initialState

    override def aggregate(event: Event, state: State): State = inner.aggregate(event, state)
  }

  case class AndParser[Event, State1, State2](left: BooleanPhaseParser[Event, State1],
                                              right: BooleanPhaseParser[Event, State2])
    extends ComparingParser(left, right) {

    override def compare(a: Boolean, b: Boolean): Boolean =
      a && b

  }

  case class OrParser[Event, State1, State2](left: BooleanPhaseParser[Event, State1],
                                             right: BooleanPhaseParser[Event, State2])
    extends ComparingParser(left, right) {

    override def compare(a: Boolean, b: Boolean): Boolean = a | b

  }

}
