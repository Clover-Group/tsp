package ru.itclover.streammachine.phases

import ru.itclover.streammachine.core.PhaseParser.WithParser
import ru.itclover.streammachine.core._
import ru.itclover.streammachine.phases.ConstantPhases.OneRowPhaseParser

import scala.Numeric.Implicits._
import scala.Predef.{any2stringadd => _, _}
import Numeric._

object NumericPhases {

  trait NumericPhasesSyntax[Event, S, T] {
    this: WithParser[Event, S, T] =>

    def +[S2](right: NumericPhaseParser[Event, S2])(implicit ev: T <:< Double): NumericPhaseParser[Event, (S, S2)] =
      NumericPhases[Event, (S, S2)]((this.parser and right).map { case (a, b) => a + b })

    def -[S2](right: NumericPhaseParser[Event, S2])(implicit ev: T <:< Double): NumericPhaseParser[Event, (S, S2)] =
      NumericPhases[Event, (S, S2)]((this.parser and right).map { case (a, b) => a - b })

    //todo Failure if b is zero?
    def /[S2](right: NumericPhaseParser[Event, S2])(implicit ev: T <:< Double): NumericPhaseParser[Event, (S, S2)] =
      NumericPhases[Event, (S, S2)]((this.parser and right).map { case (a, b) => a / b })

    def *[S2](right: NumericPhaseParser[Event, S2])(implicit ev: T <:< Double): NumericPhaseParser[Event, (S, S2)] =
      NumericPhases[Event, (S, S2)]((this.parser and right).map { case (a, b) => a * b })
  }

  trait PhaseGetter[Event] extends (() => PhaseParser[Event, _, _]) with Serializable {
    def getPhase()
  }

  type ConstantPhaseParser[Event, +T] = OneRowPhaseParser[Event, T]

  type NumericPhaseParser[Event, S] = PhaseParser[Event, S, Double]

  trait SymbolNumberExtractor[Event] extends Serializable {
    def extract(event: Event, symbol: Symbol): Double
  }

  implicit def doubleExtractor[E](value: Double): NumericPhaseParser[E, Unit] = NumericPhases(ConstantPhases[E, Double](value))

  implicit def floatExtractor[E](value: Float): NumericPhaseParser[E, Unit] = NumericPhases(ConstantPhases[E, Double](value.toDouble))

  implicit def intExtractor[E](value: Int): NumericPhaseParser[E, Unit] = NumericPhases(ConstantPhases[E, Double](value.toDouble))

  implicit def longExtractor[E](value: Long): NumericPhaseParser[E, Unit] = NumericPhases(ConstantPhases[E, Double](value.toDouble))

  implicit def functionNumberExtractor[Event, N: Numeric](f: Event => N): NumericPhaseParser[Event, Unit] = NumericPhases(OneRowPhaseParser(f.andThen(_.toDouble())))

  def apply[Event, State](inner: PhaseParser[Event, State, Double]): NumericPhaseParser[Event, State] =
    new NumericPhaseParser[Event, State] {
      override def initialState = inner.initialState

      override def apply(v1: Event, v2: State) = inner.apply(v1, v2)
    }

  implicit class SymbolParser(val symbol: Symbol) extends AnyVal {

    def field[Event: SymbolNumberExtractor]: NumericPhaseParser[Event, Unit] = NumericPhases(OneRowPhaseParser(e => implicitly[SymbolNumberExtractor[Event]].extract(e, symbol)))
  }


  def value[E, N: Numeric](n: N): NumericPhaseParser[E, Unit] = NumericPhases(ConstantPhases[E, Double](n.toDouble))

}
