package ru.itclover.streammachine.phases

import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult}
import ru.itclover.streammachine.phases.NumericPhases.NumericPhaseParser
import scala.language.implicitConversions

object ConstantPhases {

  def apply[Event, T](value: T): PhaseParser[Event, NoState, T] = new OneRowPhaseParser[Event, T] {
    override def extract(event: Event): T = value
  }

  trait OneRowPhaseParser[Event, +T] extends PhaseParser[Event, NoState, T] {

    override def apply(v1: Event, v2: NoState): (PhaseResult[T], NoState) = {
      val value = extract(v1)
      (if (value != null && value != Double.NaN) Success(value) else Stay) -> NoState.instance
    }

    override def aggregate(event: Event, state: NoState): NoState = initialState

    def extract(event: Event): T

    def fieldName: Option[String] = None

    override def initialState: NoState = NoState.instance

    override def format(event: Event, state: NoState) = if (fieldName.isDefined) {
      s"${fieldName.get}=${extract(event)}"
    } else {
      s"${extract(event)}"
    }
  }

  object OneRowPhaseParser {
    def apply[Event, T](f: Event => T, fieldNameOpt: Option[String] = None): OneRowPhaseParser[Event, T] = new OneRowPhaseParser[Event, T]() {
      override val fieldName = fieldNameOpt

      override def extract(event: Event) = f(event)
    }
  }

  case class FailurePhaseParser[Event](msg: String) extends OneRowPhaseParser[Event, Nothing] {
    override def apply(v1: Event, v2: NoState): (Failure, NoState) = Failure(msg) -> NoState.instance

    override def extract(event: Event): Nothing = ???

    override def format(event: Event, state: NoState) = s"Failure($msg)"
  }

  trait LessPriorityImplicits {

    implicit def value[E, T](n: T): PhaseParser[E, NoState, T] = ConstantPhases[E, T](n)

  }

  trait ConstantFunctions extends LessPriorityImplicits {

    import scala.Numeric.Implicits._

    implicit def doubleExtractor[E](value: Double): NumericPhaseParser[E, NoState] = ConstantPhases[E, Double](value)

    implicit def floatExtractor[E](value: Float): NumericPhaseParser[E, NoState] = ConstantPhases[E, Double](value.toDouble)

    implicit def longExtractor[E](value: Long): PhaseParser[E, NoState, Long] = ConstantPhases[E, Long](value)

    implicit def intExtractor[E](value: Int): PhaseParser[E, NoState, Int] = ConstantPhases[E, Int](value)

    implicit def extract[Event, T](f: Event => T): PhaseParser[Event, NoState, T] = OneRowPhaseParser(f)
  }

}
