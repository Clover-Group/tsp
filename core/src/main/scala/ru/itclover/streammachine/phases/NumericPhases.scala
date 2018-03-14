package ru.itclover.streammachine.phases

import ru.itclover.streammachine
import ru.itclover.streammachine.core.PhaseParser.WithParser
import ru.itclover.streammachine.core._
import ru.itclover.streammachine.phases
import ru.itclover.streammachine.phases.CombiningPhases.TogetherParserLike
import ru.itclover.streammachine.phases.ConstantPhases.OneRowPhaseParser
import scala.Numeric.Implicits._
import scala.Fractional.Implicits._
import scala.Predef.{any2stringadd => _, _}


object NumericPhases {

  trait NumericPhasesSyntax[Event, S, T] {
    this: WithParser[Event, S, T] =>

    def +[S2](right: PhaseParser[Event, S2, T])(implicit ev: Numeric[T]) =
      BinaryNumericParser(this.parser, right, (a: T, b: T) => ev.plus(a, b), "+")

    def -[S2](right: PhaseParser[Event, S2, T])(implicit ev: Numeric[T]) =
      BinaryNumericParser(this.parser, right, (a: T, b: T) => ev.minus(a, b), "-")

    //todo Failure if b is zero?
    def /[S2](right: PhaseParser[Event, S2, T])(implicit ev: Fractional[T]) =
      BinaryNumericParser(this.parser, right, (a: T, b: T) => ev.div(a, b), "/")

    def *[S2](right: PhaseParser[Event, S2, T])(implicit ev: Numeric[T]) =
      BinaryNumericParser(this.parser, right, (a: T, b: T) => ev.times(a, b), "*")
  }


  case class BinaryNumericParser[E, S1, S2, Out](left: PhaseParser[E, S1, Out], right: PhaseParser[E, S2, Out],
                                                 operation: (Out, Out) => Out, operationSign: String)
                                                (implicit innerEv: Numeric[Out])
       extends PhaseParser[E, (S1, S2), Out] {

    val andParser = left togetherWith right

    override def initialState = (left.initialState, right.initialState)

    override def apply(event: E, state: (S1, S2)): (PhaseResult[Out], (S1, S2)) = {
      val (results, newState) = andParser(event, state)
      results.map { case (a, b) => operation(a, b) } -> newState
    }

    override def format(e: E, st: (S1, S2)) = left.format(e, st._1) + s" $operationSign " + right.format(e, st._2)
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

    def field[Event: SymbolNumberExtractor]: NumericPhaseParser[Event, NoState] = OneRowPhaseParser(e => implicitly[SymbolNumberExtractor[Event]].extract(e, symbol), Some(symbol.toString))
  }

  implicit class SymbolParser[Event](val symbol: Symbol) extends AnyVal with Serializable {

    def as[T](implicit ev: SymbolExtractor[Event, T]): ConstantPhaseParser[Event, T] =
      OneRowPhaseParser[Event, T](e => ev.extract(e, symbol), Some(symbol.toString))
  }

}
