package ru.itclover.tsp.phases

import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}
import ru.itclover.tsp.core.{Pattern, PatternResult}
import ru.itclover.tsp.phases.NumericPhases.NumericPhaseParser
import scala.language.implicitConversions

object ConstantPhases {

  def apply[Event, T](value: T): OneRowPattern[Event, T] = new OneRowPattern[Event, T] {
    override def extract(event: Event): T = value
  }

  trait OneRowPattern[Event, +T] extends Pattern[Event, NoState, T] {

    override def apply(v1: Event, v2: NoState): (PatternResult[T], NoState) = {
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

  object OneRowPattern {
    def apply[Event, T](f: Event => T, fieldNameOpt: Option[String] = None): OneRowPattern[Event, T] = new OneRowPattern[Event, T]() {
      override val fieldName = fieldNameOpt

      override def extract(event: Event) = f(event)
    }
  }

  case class FailurePattern[Event](msg: String) extends OneRowPattern[Event, Nothing] {
    override def apply(v1: Event, v2: NoState): (Failure, NoState) = Failure(msg) -> NoState.instance

    override def extract(event: Event): Nothing = ???

    override def format(event: Event, state: NoState) = s"Failure($msg)"
  }

  trait LessPriorityImplicits {

    implicit def value[E, T](n: T): Pattern[E, NoState, T] = ConstantPhases[E, T](n)

  }

  trait ConstantFunctions extends LessPriorityImplicits {

    import scala.Numeric.Implicits._

    implicit def doubleExtractor[E](value: Double): NumericPhaseParser[E, NoState] = ConstantPhases[E, Double](value)

    implicit def floatExtractor[E](value: Float): NumericPhaseParser[E, NoState] = ConstantPhases[E, Double](value.toDouble)

    implicit def longExtractor[E](value: Long): Pattern[E, NoState, Long] = ConstantPhases[E, Long](value)

    implicit def intExtractor[E](value: Int): Pattern[E, NoState, Int] = ConstantPhases[E, Int](value)

    implicit def extract[Event, T](f: Event => T): Pattern[Event, NoState, T] = OneRowPattern(f)
  }

}
