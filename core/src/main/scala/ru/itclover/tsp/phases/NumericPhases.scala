package ru.itclover.tsp.phases

import ru.itclover.tsp.core.Pattern.WithPattern
import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.core._
import ru.itclover.tsp.phases.ConstantPhases.OneRowPattern

import scala.Numeric.Implicits._
import scala.Fractional.Implicits._
import scala.Predef.{any2stringadd => _, _}
import shapeless.{HList, Poly, Poly1}
import shapeless.UnaryTCConstraint.*->*
import shapeless.ops.hlist.{Comapped, Mapped, NatTRel}

object NumericPhases {

  trait NumericPatternsSyntax[Event, S, T] {
    this: WithPattern[Event, S, T] =>

    def plus[S2](right: Pattern[Event, S2, T])(implicit ev: Numeric[T]) =
      BinaryNumericParser(this.parser, right, (a: T, b: T) => ev.plus(a, b), "plus")

    def minus[S2](right: Pattern[Event, S2, T])(implicit ev: Numeric[T]) =
      BinaryNumericParser(this.parser, right, (a: T, b: T) => ev.minus(a, b), "minus")

    def times[S2](right: Pattern[Event, S2, T])(implicit ev: Numeric[T]) =
      BinaryNumericParser(this.parser, right, (a: T, b: T) => ev.times(a, b), "times")

    //todo Failure if b is zero?
    def div[S2](right: Pattern[Event, S2, T])(implicit ev: Fractional[T]) =
      BinaryNumericParser(this.parser, right, (a: T, b: T) => ev.div(a, b), "div")
  }

  trait NumericFunctions {

    def abs[Event, State](numeric: NumericPhaseParser[Event, State])(implicit timeExtractor: TimeExtractor[Event]) =
      AbsPhase(numeric) // TODO map(_.abs) with format in Writer monad

    def call1[Event, State](
      function: Double => Double,
      functionName: String,
      innerPhase1: NumericPhaseParser[Event, State]
    )(
      implicit timeExtractor: TimeExtractor[Event]
    ) =
      Function1Phase(function, functionName, innerPhase1)

    def call2[Event, State1, State2](
      function: (Double, Double) => Double,
      functionName: String,
      innerPhase1: NumericPhaseParser[Event, State1],
      innerPhase2: NumericPhaseParser[Event, State2]
    )(implicit timeExtractor: TimeExtractor[Event]) =
      Function2Phase(function, functionName, innerPhase1, innerPhase2)
  }

  case class Reduce[Event, State](reducer: (Double, Double) => Double)(
    val firstPhase: NumericPhaseParser[Event, State],
    val otherPhases: NumericPhaseParser[Event, State]*
  ) extends NumericPhaseParser[Event, Seq[State]] {
    override def initialState: Seq[State] = firstPhase.initialState +: otherPhases.map(_.initialState)

    override def apply(event: Event, states: Seq[State]): (PatternResult[Double], Seq[State]) = {
      val (firstResult, firstState) = firstPhase.apply(event, states.head)
      // Reduce results and accumulate states, or return first available result
      otherPhases.zip(states.tail).foldLeft((firstResult, Seq(firstState))) {
        case ((maxResult, accumState), (phase, state)) => {
          val (newResult, newState) = phase(event, state)
          val newMax = reduceResults(maxResult, newResult)
          (newMax, accumState :+ newState)
        }
      }
    }

    override def format(event: Event, states: Seq[State]) = {
      val phasesWithState = (firstPhase, states.head) +: otherPhases.zip(states.tail)
      val numericsResults = (phasesWithState
        .map {
          case (phase, state) => phase.format(event, state)
        })
        .mkString(", ")
      apply(event, states)._1 match {
        case Success(result) => s"Reduce($numericsResults)=$result"
        case Failure(err)    => s"Reduce($numericsResults)=Failure($err)"
        case _               => s"Reduce($numericsResults)"
      }
    }

    @inline private def reduceResults(
      current: PatternResult[Double],
      next: PatternResult[Double]
    ): PatternResult[Double] =
      (current, next) match {
        case (Success(curr), Success(newNum)) => Success(reducer(curr, newNum))
        case (fail: Failure, _)               => fail
        case (_, fail: Failure)               => fail
        case _                                => Stay
      }
  }

  case class BinaryNumericParser[E, S1, S2, Out](
    left: Pattern[E, S1, Out],
    right: Pattern[E, S2, Out],
    operation: (Out, Out) => Out,
    operationSign: String
  )(implicit innerEv: Numeric[Out])
      extends Pattern[E, (S1, S2), Out] {

    val andParser = left togetherWith right

    override def initialState = (left.initialState, right.initialState)

    override def apply(event: E, state: (S1, S2)): (PatternResult[Out], (S1, S2)) = {
      val (results, newState) = andParser(event, state)
      results.map { case (a, b) => operation(a, b) } -> newState
    }

    override def format(e: E, st: (S1, S2)) = left.format(e, st._1) + s" $operationSign " + right.format(e, st._2)
  }

  type ConstantPhaseParser[Event, +T] = OneRowPattern[Event, T]

  type NumericPhaseParser[Event, S] = Pattern[Event, S, Double]

  trait SymbolNumberExtractor[Event] extends SymbolExtractor[Event, Double] {
    def extract(event: Event, symbol: Symbol): Double
  }

  trait SymbolExtractor[Event, T] extends Serializable {
    def extract(event: Event, symbol: Symbol): T
  }

  implicit class SymbolNumberParser(val symbol: Symbol) extends AnyVal with Serializable {

    def field[Event: SymbolNumberExtractor]: NumericPhaseParser[Event, NoState] =
      OneRowPattern(e => implicitly[SymbolNumberExtractor[Event]].extract(e, symbol), Some(symbol.toString))
  }

  implicit class SymbolParser[Event](val symbol: Symbol) extends AnyVal with Serializable {

    def as[T](implicit ev: SymbolExtractor[Event, T]): ConstantPhaseParser[Event, T] =
      OneRowPattern[Event, T](e => ev.extract(e, symbol), Some(symbol.toString))
  }

  // TODO@trolley813: replace with a generic function
  case class AbsPhase[Event, State](numeric: NumericPhaseParser[Event, State]) extends NumericPhaseParser[Event, State] {

    override def apply(event: Event, state: State) = {
      val (innerResult, newState) = numeric(event, state)
      (innerResult match {
        case Success(x) => Success(Math.abs(x))
        case x          => x
      }) -> newState
    }

    override def initialState = numeric.initialState

    override def format(event: Event, state: State) =
      s"abs(${numeric.format(event, state)})"
  }

  case class Function1Phase[Event, State](
    function: Double => Double,
    functionName: String,
    phase1: NumericPhaseParser[Event, State]
  ) extends NumericPhaseParser[Event, State] {

    override def apply(event: Event, state: State): (PatternResult[Double], State) = {
      val (innerResult, newState) = phase1(event, state)
      (innerResult match {
        case Success(x) => Success(function(x))
        case x          => x
      }) -> newState
    }

    override def initialState: State = phase1.initialState

    override def format(event: Event, state: State) =
      s"$functionName(${phase1.format(event, state)})"
  }

  case class Function2Phase[Event, State1, State2](
    function: (Double, Double) => Double,
    functionName: String,
    phase1: NumericPhaseParser[Event, State1],
    phase2: NumericPhaseParser[Event, State2]
  ) extends Pattern[Event, (State1, State2), Double] {

    override def apply(event: Event, state: (State1, State2)): (PatternResult[Double], (State1, State2)) = {
      val (results, newState) = (phase1 togetherWith phase2)(event, state)
      results.map { case (x1, x2) => function(x1, x2) } -> newState
    }

    override def initialState: (State1, State2) = (phase1.initialState, phase2.initialState)

    override def format(event: Event, state: (State1, State2)): String =
      s"$functionName(${phase1.format(event, state._1)}, ${phase2.format(event, state._2)})"
  }

  case class FunctionNPhase[Event, States <: HList, Phases <: HList](
    function: Seq[Double] => Double,
    functionName: String,
    innerPhases: Phases
  )(implicit ev: Mapped.Aux[States, ({ type T[State] = NumericPhaseParser[Event, State] })#T, Phases])
      extends Pattern[Event, States, Double] {
    override def initialState: States = innerPhases.map(new Poly1 {
      implicit val pat = at[Pattern[Event, _, _]] { p =>
        p.initialState
      }
    })
    override def apply(v1: Event, v2: Phases): (PatternResult[Double], Phases) = {
      val allPhasesTogether =
        innerPhases.foldLeft((f: NumericPhaseParser[Event, _], g: NumericPhaseParser[Event, _]) => f togetherWith g)
      val (results, newState) = allPhasesTogether(event, state)
      // FIXME@trolley813: it's pseudocode, won't work
      results.map { case x => function(x.toSeq) } -> newState

    }
  }
}
