package ru.itclover.streammachine.phases

import ru.itclover.streammachine
import ru.itclover.streammachine.core.PhaseParser.WithParser
import ru.itclover.streammachine.core._
import ru.itclover.streammachine.phases
import ru.itclover.streammachine.phases.ConstantPhases.OneRowPhaseParser

import scala.Numeric.Implicits._
import scala.Predef.{any2stringadd => _, _}
import Numeric._

object NumericPhases {

  trait NumericPhasesSyntax[Event, S, T] {
    this: WithParser[Event, S, T] =>

    def +[S2](right: NumericPhaseParser[Event, S2])(implicit ev: T <:< Double): NumericPhaseParser[Event, (S, S2)] =
      NumericPhases[Event, (S, S2)]((this.parser togetherWith right).map { case (a, b) => a + b })

    def -[S2](right: NumericPhaseParser[Event, S2])(implicit ev: T <:< Double): NumericPhaseParser[Event, (S, S2)] =
      NumericPhases[Event, (S, S2)]((this.parser togetherWith right).map { case (a, b) => a - b })

    //todo Failure if b is zero?
    def /[S2](right: NumericPhaseParser[Event, S2])(implicit ev: T <:< Double): NumericPhaseParser[Event, (S, S2)] =
      NumericPhases[Event, (S, S2)]((this.parser togetherWith right).map { case (a, b) => a / b })

    def *[S2](right: NumericPhaseParser[Event, S2])(implicit ev: T <:< Double): NumericPhaseParser[Event, (S, S2)] =
      NumericPhases[Event, (S, S2)]((this.parser togetherWith right).map { case (a, b) => a * b })
  }

  trait PhaseGetter[Event] extends (() => PhaseParser[Event, _, _]) with Serializable {
    def getPhase()
  }

  type ConstantPhaseParser[Event, +T] = OneRowPhaseParser[Event, T]

  type NumericPhaseParser[Event, S] = PhaseParser[Event, S, Double]

  trait SymbolNumberExtractor[Event] extends SymbolExtractor[Event, Double] {
    def extract(event: Event, symbol: Symbol): Double
  }

  trait SymbolExtractor[Event, T] extends Serializable {
    def extract(event: Event, symbol: Symbol): T
  }

  def apply[Event, State](inner: PhaseParser[Event, State, Double]): NumericPhaseParser[Event, State] =
    new NumericPhaseParser[Event, State] {
      override def initialState = inner.initialState

      override def apply(v1: Event, v2: State) = inner.apply(v1, v2)

      override def aggregate(v1: Event, v2: State) = inner.aggregate(v1, v2)
    }

  implicit class SymbolNumberParser(val symbol: Symbol) extends AnyVal {

    def field[Event: SymbolNumberExtractor]: NumericPhaseParser[Event, Unit] = NumericPhases(OneRowPhaseParser(e => implicitly[SymbolNumberExtractor[Event]].extract(e, symbol)))
  }

  implicit class SymbolParser[Event](val symbol: Symbol) extends AnyVal {

    def as[T](implicit ev: SymbolExtractor[Event, T]): ConstantPhaseParser[Event, T] = OneRowPhaseParser[Event, T](e => implicitly[SymbolExtractor[Event, T]].extract(e, symbol))
  }

}
