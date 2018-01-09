package ru.itclover.streammachine.core

import ru.itclover.streammachine.core.NumericPhaseParser._

import scala.Numeric.Implicits._
import scala.Predef.{any2stringadd => _, _}


trait NumericPhaseParser[Event, S] extends PhaseParser[Event, S, Double] with Serializable {

  def +[S2](right: NumericPhaseParser[Event, S2]): NumericPhaseParser[Event, (S, S2)] =
    NumericPhaseParser[Event, (S, S2)]((this and right).map { case (a, b) => a + b })

  def -[S2](right: NumericPhaseParser[Event, S2]): NumericPhaseParser[Event, (S, S2)] =
    NumericPhaseParser[Event, (S, S2)]((this and right).map { case (a, b) => a - b })

  //todo Failure if b is zero?
  def /[S2](right: NumericPhaseParser[Event, S2]): NumericPhaseParser[Event, (S, S2)] =
    NumericPhaseParser[Event, (S, S2)]((this and right).map { case (a, b) => a / b })

  def *[S2](right: NumericPhaseParser[Event, S2]): NumericPhaseParser[Event, (S, S2)] =
    NumericPhaseParser[Event, (S, S2)]((this and right).map { case (a, b) => a * b })

  def >[S2](right: NumericPhaseParser[Event, S2]) = assertParser(this and right)({ case (a, b) => a > b })

  def >=[S2](right: NumericPhaseParser[Event, S2]) = assertParser(this and right)({ case (a, b) => a >= b })

  def <[S2](right: NumericPhaseParser[Event, S2]) = assertParser(this and right)({ case (a, b) => a < b })

  def <=[S2](right: NumericPhaseParser[Event, S2]) = assertParser(this and right)({ case (a, b) => a <= b })

  def ===[S2](right: NumericPhaseParser[Event, S2]) = assertParser(this and right)({ case (a, b) => a == b })

  def !=[S2](right: NumericPhaseParser[Event, S2]) = assertParser(this and right)({ case (a, b) => a != b })
}


object NumericPhaseParser {

  trait PhaseGetter[Event] extends (() => PhaseParser[Event, _, _]) with Serializable {
    def getPhase()
  }

  type BooleanPhaseParser[Event, State] = PhaseParser[Event, State, Boolean]

  type ConstantPhaseParser[Event, +T] = OneRowPhaseParser[Event, T]

  type NumericPhaseParser2[Event, S] = PhaseParser[Event, S, Double]

  trait SymbolNumberExtractor[Event] extends Serializable {
    def extract(event: Event, symbol: Symbol): Double
  }

  implicit def doubleExtractor[E](value: Double): NumericPhaseParser[E, Unit] = NumericPhaseParser(ConstantPhaseParser[E, Double](value))

  implicit def floatExtractor[E](value: Float): NumericPhaseParser[E, Unit] = NumericPhaseParser(ConstantPhaseParser[E, Double](value.toDouble))

  implicit def intExtractor[E](value: Int): NumericPhaseParser[E, Unit] = NumericPhaseParser(ConstantPhaseParser[E, Double](value.toDouble))

  implicit def longExtractor[E](value: Long): NumericPhaseParser[E, Unit] = NumericPhaseParser(ConstantPhaseParser[E, Double](value.toDouble))

  implicit def functionNumberExtractor[Event, N: Numeric](f: Event => N): NumericPhaseParser[Event, Unit] = NumericPhaseParser(OneRowPhaseParser(f.andThen(_.toDouble())))

  def apply[Event, State](inner: PhaseParser[Event, State, Double]): NumericPhaseParser[Event, State] =
    new NumericPhaseParser[Event, State] {
      override def initialState = inner.initialState

      override def apply(v1: Event, v2: State) = inner.apply(v1, v2)
    }

  /**
    * PhaseParser returning only Success(true), Failure and Stay. Cannot return Success(false)
    *
    * @param condition - inner boolean parser. Resulted parser returns Failure if condition returned Success(true)
    * @tparam Event - event type
    * @tparam State - possible inner state
    */
  def assertParser[Event, State](condition: BooleanPhaseParser[Event, State]) =
    condition.flatMap[Unit, Boolean](b => if (b) ConstantPhaseParser[Event, Boolean](true)
                                          else FailurePhaseParser("not match"))

  /**
    * PhaseParser returning only Success(true), Failure and Stay. Cannot return Success(false)
    *
    * @param inner - inner parser.
    * @tparam Event - event type
    * @tparam State - possible inner state
    */
  def assertParser[Event, State, Out](inner: PhaseParser[Event, State, Out])(predicate: Out => Boolean):
  FlatMapParser[Event, State, Unit, Out, Boolean] =
    inner.flatMap(out => if (predicate(out)) ConstantPhaseParser(true) else FailurePhaseParser[Event]("not match"))

  implicit def field[Event: SymbolNumberExtractor](symbol: Symbol): NumericPhaseParser[Event, Unit] = NumericPhaseParser(OneRowPhaseParser(e => implicitly[SymbolNumberExtractor[Event]].extract(e, symbol)))

  def value[E, N: Numeric](n: N): NumericPhaseParser[E, Unit] = NumericPhaseParser(ConstantPhaseParser[E, Double](n.toDouble))

}
