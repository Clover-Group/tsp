package ru.itclover.tsp.phases

import ru.itclover.tsp.core.Pattern.WithPattern
import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}
import ru.itclover.tsp.core._
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.phases.ConstantPhases.OneRowPattern
import shapeless.PolyDefns.{~>, ~>>}
import scala.Numeric.Implicits._
import scala.Fractional.Implicits._
import scala.Predef.{any2stringadd => _, _}
import shapeless.{HList, Id, Poly, Poly1}
import shapeless.ops.hlist.Mapped
import shapeless.Poly._
import shapeless.HList._
import shapeless.ops.hlist.Mapper

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
      val numericsResults = phasesWithState.map {
          case (phase, state) => phase.format(event, state)
        }
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
    innerPhase1: NumericPhaseParser[Event, State]
  ) extends NumericPhaseParser[Event, State] {

    override def apply(event: Event, state: State): (PatternResult[Double], State) = {
      val (innerResult, newState) = innerPhase1(event, state)
      (innerResult match {
        case Success(x) => Success(function(x))
        case x          => x
      }) -> newState
    }

    override def initialState: State = innerPhase1.initialState

    override def format(event: Event, state: State) =
      s"$functionName(${ innerPhase1.format(event, state)})"
  }

  case class Function2Phase[Event, State1, State2](
    function: (Double, Double) => Double,
    functionName: String,
    innerPhase1: NumericPhaseParser[Event, State1],
    innerPhase2: NumericPhaseParser[Event, State2]
  ) extends Pattern[Event, (State1, State2), Double] {

    override def apply(event: Event, state: (State1, State2)): (PatternResult[Double], (State1, State2)) = {
      val (results, newState) = (innerPhase1 togetherWith innerPhase2)(event, state)
      results.map { case (x1, x2) => function(x1, x2) } -> newState
    }

    override def initialState: (State1, State2) = (innerPhase1.initialState, innerPhase2.initialState)

    override def format(event: Event, state: (State1, State2)): String =
      s"$functionName(${ innerPhase1.format(event, state._1)}, ${ innerPhase2.format(event, state._2)})"
  }

  // TODO FunctionNPhase: https://gist.github.com/kell18/05261e56504da16d5db8384a4ad65733 
}
