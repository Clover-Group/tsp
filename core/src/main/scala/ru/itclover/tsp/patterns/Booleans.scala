package ru.itclover.tsp.patterns

import ru.itclover.tsp.core.Pattern.WithPattern
import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}
import ru.itclover.tsp.core._
import ru.itclover.tsp.patterns.Monads.MapParserLike
import ru.itclover.tsp.utils.UtilityTypes.And

object Booleans {

  trait BooleanSyntax[Event, S, T] {
    this: WithPattern[Event, S, T] =>

    def >[S2](right: Pattern[Event, S2, T])(implicit ord: Ordering[T]) =
      ComparingPattern(this.parser, right)((a, b) => ord.gt(a, b), ">")

    def >=[S2](right: Pattern[Event, S2, T])(implicit ord: Ordering[T]) =
      ComparingPattern(this.parser, right)((a, b) => ord.gteq(a, b), ">=")

    def <[S2](right: Pattern[Event, S2, T])(implicit ord: Ordering[T]) =
      ComparingPattern(this.parser, right)((a, b) => ord.lt(a, b), "<")

    def <=[S2](right: Pattern[Event, S2, T])(implicit ord: Ordering[T]) =
      ComparingPattern(this.parser, right)((a, b) => ord.lteq(a, b), "<=")

    def and[S2](right: BooleanPhaseParser[Event, S2])(implicit ev: T =:= Boolean) =
      ComparingPattern(this.parser.asInstanceOf[BooleanPhaseParser[Event, S]], right)((a, b) => a & b, "and")

    def or[S2](right: BooleanPhaseParser[Event, S2])(implicit ev: T =:= Boolean) =
      ComparingPattern(this.parser.asInstanceOf[BooleanPhaseParser[Event, S]], right)((a, b) => a | b, "or")

    def xor[S2](right: BooleanPhaseParser[Event, S2])(implicit ev: T =:= Boolean) =
      ComparingPattern(this.parser.asInstanceOf[BooleanPhaseParser[Event, S]], right)((a, b) => a ^ b, "xor")

    /**
      * Alias for `and`
      */
    def &[RightState](rightParser: BooleanPhaseParser[Event, RightState])(
      implicit ev: T =:= Boolean
    ): ComparingPattern[Event, S, RightState, Boolean] = and(rightParser)

    /**
      * Alias for `or`
      */
    def |[RightState](rightParser: BooleanPhaseParser[Event, RightState])(
      implicit ev: T =:= Boolean
    ): ComparingPattern[Event, S, RightState, Boolean] = or(rightParser)

    def ===[S2](right: Pattern[Event, S2, T]) = ComparingPattern(this.parser, right)((a, b) => a equals b, "==")

    def =!=[S2](right: Pattern[Event, S2, T]) = ComparingPattern(this.parser, right)((a, b) => !(a equals b), "!=")

    //todo should it be re-written to accept Pattern[?, ?, Set[?]] ?
    def in(set: Set[T]) = InParser(this.parser, set)

  }

  trait BooleanFunctions {
    def not[Event, S](inner: BooleanPhaseParser[Event, S]): NotParser[Event, S] = NotParser(inner)
  }

  type BooleanPhaseParser[Event, State] = Pattern[Event, State, Boolean]

  /**
    * Pattern returning only Success(true), Failure and Stay. Cannot return Success(false)
    *
    * * @param predicate - inner boolean parser. Resulted parser returns Failure if predicate returned Success(false)
    * * @tparam Event - event type
    * * @tparam State - possible inner state
    */
  case class Assert[Event, State](predicate: BooleanPhaseParser[Event, State]) extends BooleanPhaseParser[Event, State] {
    override def apply(event: Event, s: State): (PatternResult[Boolean], State) = {
      val (res, out) = predicate(event, s)
      res.flatMap(r => if (r) Success(true) else Failure("Assert not match")) -> out
    }

    override def initialState: State = predicate.initialState

    override def format(event: Event, state: State) = {
      s"Assert(${predicate.format(event, state)})"
    }
  }

  case class ComparingPattern[Event, S1, S2, T](left: Pattern[Event, S1, T], right: Pattern[Event, S2, T])(
    compare: (T, T) => Boolean,
    compareFnName: String
  ) extends BooleanPhaseParser[Event, S1 And S2] {

    val comparingFunction: (T, T) => Boolean = compare
    val comparingFunctionName: String = compareFnName

    private val andParser = left togetherWith right

    override def apply(e: Event, state: (S1, S2)): (PatternResult[Boolean], (S1, S2)) = {
      val (res, newState) = andParser(e, state)

      res.map { case (a, b) => compare(a, b) } -> newState
    }

    override def aggregate(event: Event, state: (S1, S2)): (S1, S2) = {
      val (leftState, rightState) = state
      left.aggregate(event, leftState) -> right.aggregate(event, rightState)
    }

    override def initialState: (S1, S2) = (left.initialState, right.initialState)

    override def format(event: Event, state: (S1, S2)): String = {
      left.format(event, state._1) + s" ${compareFnName} " + right.format(event, state._2)
    }
  }

  case class NotParser[Event, State](inner: BooleanPhaseParser[Event, State]) extends BooleanPhaseParser[Event, State] {

    override def apply(e: Event, state: State): (PatternResult[Boolean], State) = {

      val (result, newState) = inner(e, state)
      (result match {
        case Success(b) => Success(!b)
        case x: Failure => x
        case Stay       => Stay
      }) -> newState
    }

    override def initialState: State = inner.initialState

    override def aggregate(event: Event, state: State): State = inner.aggregate(event, state)

    override def format(event: Event, state: State) = s"not ${inner.format(event, state)}"
  }

  case class InParser[Event, State, T](parser: Pattern[Event, State, T], set: Set[T])
      extends MapParserLike(parser)(set.apply)
      with BooleanPhaseParser[Event, State]

}
