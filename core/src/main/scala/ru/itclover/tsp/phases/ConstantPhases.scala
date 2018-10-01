package ru.itclover.tsp.phases

import ru.itclover.tsp.core.PatternResult.{Failure, Stay, Success}
import ru.itclover.tsp.core.{Pattern, PatternResult}
import ru.itclover.tsp.phases.NumericPhases.NumericPhaseParser
import scala.language.implicitConversions

object ConstantPhases {

  def apply[Event, T](value: T): OneRowPattern[Event, T] = OneRowPattern[Event, T](_ => value)

  trait OneRowMarker[Event, +T] {
    def fieldName: Option[String] = None

    def extract(event: Event): T
  }

  type OneRowPattern[Event, +T] = Pattern[Event, NoState, T] with OneRowMarker[Event, T]

  object OneRowPattern {
    def construct[Event, T](extractF: Event => T,
                            fieldName: => Option[String] = Option.empty)(
                             formatF: (Event, NoState) => String = (event: Event, noState: NoState) => {
                               if (fieldName.isDefined) {
                                 s"${fieldName.get}=${extractF(event)}"
                               } else {
                                 s"${extractF(event)}"
                               }
                             }
                           ): OneRowPattern[Event, T]
    = new Pattern[Event, NoState, T] with OneRowMarker[Event, T] {
      override def apply(v1: Event, v2: NoState): (PatternResult[T], NoState) = {
        val value = extract(v1)
        (if (value != null && value != Double.NaN) Success(value) else Stay) -> NoState.instance
      }

      override def aggregate(event: Event, state: NoState): NoState = initialState

      override def fieldName: Option[String] = fieldName

      override def initialState: NoState = NoState.instance

      override def format(event: Event, state: NoState): String = formatF(event, state)

      override def extract(event: Event): T = extractF(event)
    }

    def apply[Event, T](f: Event => T, fieldNameOpt: Option[String] = Option.empty): OneRowPattern[Event, T] =
      construct[Event, T](f, fieldNameOpt)()
  }

  object FailurePattern {
    def apply[Event](msg: String): OneRowPattern[Event, Nothing] = OneRowPattern.construct[Event, Nothing](???.asInstanceOf[Event => Nothing])((e: Event, n: NoState) => s"Failure($msg)")
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
