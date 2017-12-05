package ru.itclover.streammachine.core

import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import Predef.{any2stringadd => _, _}


trait Extract[Event] extends (Event => PhaseResult[Double]) {

  type NumberPhaseParser[T, S] = PhaseParser[T, S, Double]

  def +(right: NumberPhaseParser[Event]): Extract[Event] = {
    val that = this.apply _
    new Extract[Event] {
      override def apply(v1: Event) = for (a <- that(v1); b <- right(v1)) yield a + b
    }
  }

  def -(right: NumberPhaseParser[Event]): NumberPhaseParser[Event] = {
    val that = this.apply _
    new Extract[Event] {
      override def apply(v1: Event) = for (a <- that(v1); b <- right(v1)) yield a - b
    }
  }

  def >(right: NumberPhaseParser[Event]): PhaseParser[Event, ] = {
    val that = this.apply _
    new Extract[Event] {
      override def apply(v1: Event) = for (a <- that(v1); b <- right(v1)) yield a - b
    }
  }

//  def


  case class Wait(condition: Event => Boolean) extends PhaseParser[Event, Unit, Boolean] {

    override def apply(event: Event, v2: Unit): (PhaseResult[Boolean], Unit) = {
      if (condition(event)) {
        Success(true) -> ()
      } else {
        Stay -> ()
      }
    }

    override def initialState: Unit = ()
  }

}


object Extract {

  trait SymbolNumberExtractor[Event] {
    def extract(event: Event, symbol: Symbol): Double
  }

  case class SymbolExtractor[Event: SymbolNumberExtractor](symbol: Symbol) extends PhaseParser[Event] {
    override def apply(v1: Event): PhaseResult[Double] = Success(implicitly[SymbolNumberExtractor[Event]].extract(v1, symbol))
  }

  implicit def symbolToExtract[Event: SymbolNumberExtractor](symbol: Symbol): SymbolExtractor[Event] = SymbolExtractor(symbol)

  import Numeric.Implicits._

  implicit case class NumberExtractor[N: Numeric](n: N) extends AnyVal with Extract[_] {
    override def apply(v1: _): PhaseResult[Double] = Success(n.toDouble)
  }

  case class avg[Event](extract: Extract[Event]) extends Extract


}
