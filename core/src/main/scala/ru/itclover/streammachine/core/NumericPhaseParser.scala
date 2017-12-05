package ru.itclover.streammachine.core

import ru.itclover.streammachine.core.NumericPhaseParser.ComparePhaseParser
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}

import Predef.{any2stringadd => _, _}


trait NumericPhaseParser[Event, S] extends PhaseParser[Event, S, Double] {

  def +[S2](right: NumericPhaseParser[Event, S2]): NumericPhaseParser[Event, (S, S2)] =
    NumericPhaseParser[Event, (S, S2)]((this and right).map { case (a, b) => a + b })

  def -[S2](right: NumericPhaseParser[Event, S2]): NumericPhaseParser[Event, (S, S2)] =
    NumericPhaseParser[Event, (S, S2)]((this and right).map { case (a, b) => a - b })

  //todo Failure if b is zero?
  def /[S2](right: NumericPhaseParser[Event, S2]): NumericPhaseParser[Event, (S, S2)] =
    NumericPhaseParser[Event, (S, S2)]((this and right).map { case (a, b) => a / b })

  def *[S2](right: NumericPhaseParser[Event, S2]): NumericPhaseParser[Event, (S, S2)] =
    NumericPhaseParser[Event, (S, S2)]((this and right).map { case (a, b) => a * b })

  def >[S2](right: NumericPhaseParser[Event, S2]) = ComparePhaseParser(this, right, _ > _)

  def >=[S2](right: NumericPhaseParser[Event, S2]) = ComparePhaseParser(this, right, _ >= _)

  def <[S2](right: NumericPhaseParser[Event, S2]) = ComparePhaseParser(this, right, _ < _)

  def <=[S2](right: NumericPhaseParser[Event, S2]) = ComparePhaseParser(this, right, _ <= _)

  def ==[S2](right: NumericPhaseParser[Event, S2]) = ComparePhaseParser(this, right, _ == _)

  def !=[S2](right: NumericPhaseParser[Event, S2]) = ComparePhaseParser(this, right, _ != _)

}


object NumericPhaseParser {

  trait TrivialNumericPhaseParser[Event] extends NumericPhaseParser[Event, Unit] {
    override def initialState: Unit = ()
  }

  trait SymbolNumberExtractor[Event] {
    def extract(event: Event, symbol: Symbol): Double
  }

  case class SymbolExtractor[Event: SymbolNumberExtractor](symbol: Symbol) extends TrivialNumericPhaseParser[Event] {

    override def apply(v1: Event, v2: Unit): (PhaseResult[Double], Unit) =
      Success(implicitly[SymbolNumberExtractor[Event]].extract(v1, symbol)) -> ()
  }

  implicit def symbolToExtract[Event: SymbolNumberExtractor](symbol: Symbol): SymbolExtractor[Event] = SymbolExtractor(symbol)

  import Numeric.Implicits._

  implicit class NumberExtractor[N: Numeric](n: N) extends TrivialNumericPhaseParser[Any] {
    override def apply(v1: Any, v2: Unit): (Success[Double], Unit) = Success(n.toDouble) -> ()
  }

  def apply[Event, State](inner: PhaseParser[Event, State, Double]): NumericPhaseParser[Event, State] =
    new NumericPhaseParser[Event, State] {
      override def initialState = inner.initialState

      override def apply(v1: Event, v2: State) = inner.apply(v1, v2)
    }

  case class ComparePhaseParser[Event, S1, S2](
                                                left: PhaseParser[Event, S1, Double],
                                                right: PhaseParser[Event, S2, Double],
                                                compare: (Double, Double) => Boolean
                                              ) extends PhaseParser[Event, (S1, S2), Boolean] {

    override def apply(v1: Event, v2: (S1, S2)): (PhaseResult[Boolean], (S1, S2)) =
      (left and right).map { case (a, b) => compare(a, b) }.apply(v1, v2)

    override def initialState: (S1, S2) = (left.initialState, right.initialState)
  }

}
