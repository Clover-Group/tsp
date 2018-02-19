package ru.itclover.streammachine.phases

import ru.itclover.streammachine.core.PhaseResult.{Failure, Success}
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult}
import ru.itclover.streammachine.phases.NumericPhases.NumericPhaseParser

object ConstantPhases {

  def apply[Event, T](value: T): PhaseParser[Event, NoState, T] = new OneRowPhaseParser[Event, T] {
    override def extract(event: Event): T = value
  }

  trait OneRowPhaseParser[Event, +T] extends PhaseParser[Event, NoState, T] {

    override def apply(v1: Event, v2: NoState): (PhaseResult[T], NoState) = Success(extract(v1)) -> NoState.instance

    override def aggregate(event: Event, state: NoState): NoState = initialState

    def extract(event: Event): T

    override def initialState: NoState = NoState.instance
  }

  object OneRowPhaseParser {
    def apply[Event, T](f: Event => T): OneRowPhaseParser[Event, T] = new OneRowPhaseParser[Event, T]() {
      override def extract(event: Event) = f(event)
    }
  }

  case class FailurePhaseParser[Event](msg: String) extends OneRowPhaseParser[Event, Nothing] {
    override def apply(v1: Event, v2: NoState): (Failure, NoState) = Failure(msg) -> NoState.instance

    override def extract(event: Event): Nothing = ???
  }

  trait LessPriorityImplicits {

    implicit def value[E, T](n: T): PhaseParser[E, NoState, T] = ConstantPhases[E, T](n)

  }

  trait ConstantFunctions extends LessPriorityImplicits {

    import scala.Numeric.Implicits._

    implicit def doubleExtractor[E](value: Double): NumericPhaseParser[E, NoState] = NumericPhases(ConstantPhases[E, Double](value))

    implicit def floatExtractor[E](value: Float): NumericPhaseParser[E, NoState] = NumericPhases(ConstantPhases[E, Double](value.toDouble))

    implicit def intExtractor[E](value: Int): NumericPhaseParser[E, NoState] = NumericPhases(ConstantPhases[E, Double](value.toDouble))

    implicit def longExtractor[E](value: Long): NumericPhaseParser[E, NoState] = NumericPhases(ConstantPhases[E, Double](value.toDouble))

    implicit def functionNumberExtractor[Event, N: Numeric](f: Event => N): NumericPhaseParser[Event, NoState] = NumericPhases(OneRowPhaseParser(f.andThen(_.toDouble())))

  }

}
