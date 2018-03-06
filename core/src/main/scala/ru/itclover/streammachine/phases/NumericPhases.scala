package ru.itclover.streammachine.phases

import ru.itclover.streammachine
import ru.itclover.streammachine.core.PhaseParser.WithParser
import ru.itclover.streammachine.core._
import ru.itclover.streammachine.phases
import ru.itclover.streammachine.phases.ConstantPhases.OneRowPhaseParser

import scala.Numeric.Implicits._
import scala.Predef.{any2stringadd => _, _}

object NumericPhases {

  trait NumericPhasesSyntax[Event, S, T] {
    this: WithParser[Event, S, T] =>

    def +[S2](right: NumericPhaseParser[Event, S2])(implicit ev: Numeric[T]): NumericPhaseParser[Event, (S, S2)] =
      (this.parser togetherWith right).map { case (a, b) => a.toDouble() + b }

    def -[S2](right: NumericPhaseParser[Event, S2])(implicit ev: Numeric[T]): NumericPhaseParser[Event, (S, S2)] =
      (this.parser togetherWith right).map { case (a, b) => a.toDouble() - b }

    //todo Failure if b is zero?
    def /[S2](right: NumericPhaseParser[Event, S2])(implicit ev: Numeric[T]): NumericPhaseParser[Event, (S, S2)] =
      (this.parser togetherWith right).map { case (a, b) => a.toDouble() / b }

    def *[S2](right: NumericPhaseParser[Event, S2])(implicit ev: Numeric[T]): NumericPhaseParser[Event, (S, S2)] =
      (this.parser togetherWith right).map { case (a, b) => a.toDouble() * b }
  }

  type ConstantPhaseParser[Event, +T] = OneRowPhaseParser[Event, T]

  type NumericPhaseParser[Event, S] = PhaseParser[Event, S, Double]

  trait SymbolNumberExtractor[Event] extends SymbolExtractor[Event, Double] {
    def extract(event: Event, symbol: Symbol): Double
  }

  trait SymbolExtractor[Event, T] extends Serializable {
    def extract(event: Event, symbol: Symbol): T
  }

  implicit class SymbolNumberParser(val symbol: Symbol) extends AnyVal with Serializable {

    def field[Event: SymbolNumberExtractor]: NumericPhaseParser[Event, NoState] = OneRowPhaseParser(e => implicitly[SymbolNumberExtractor[Event]].extract(e, symbol))
  }

  implicit class SymbolParser[Event](val symbol: Symbol) extends AnyVal with Serializable {

    def as[T](implicit ev: SymbolExtractor[Event, T]): ConstantPhaseParser[Event, T] =
      OneRowPhaseParser[Event, T](e => ev.extract(e, symbol), Some(symbol.toString))
  }

}
