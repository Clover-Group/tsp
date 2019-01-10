package ru.itclover.tsp.dsl.v2
import ru.itclover.tsp.core.Intervals.{Interval, TimeInterval}
import ru.itclover.tsp.core.Window

sealed trait AST extends Product with Serializable

case class Constant[T](value: T) extends AST

case class Identifier(value: Symbol) extends AST

case class Range[T](from: T, to: T) extends AST

case class FunctionCall[RT](functionName: Symbol, arguments: AST*) extends AST

case class FilteredFunctionCall[RT](functionName: Symbol, cond: Double => Boolean, arguments: AST*) extends AST

case class PatternStatsCall[RT](functionName: Symbol, inner: AST, window: Window) extends AST

case class AggregateCall[RT](functionName: Symbol, value: AST, window: Window) extends AST

case class AndThen(first: AST, second: AST) extends AST

case class Timer(cond: AST, interval: TimeInterval) extends AST

case class For(accum: AST, range: Interval[Long], exactly: Option[Boolean]) extends AST

case class Assert(cond: AST) extends AST