package ru.itclover.tsp.dsl.v2
import ru.itclover.tsp.core.Intervals.{Interval, TimeInterval}
import ru.itclover.tsp.core.Window
import ru.itclover.tsp.v2.Result

sealed trait AST[+T] extends Product with Serializable

case class Constant[T](value: T) extends AST[T]

// TODO: Other types for columns
case class Identifier(value: Symbol) extends AST[Double]

case class Range[T](from: T, to: T) extends AST[T]

case class FunctionCall[RT](functionName: Symbol, arguments: AST[Any]*) extends AST[RT]

case class FilteredFunctionCall[RT](functionName: Symbol, cond: Result[Any] => Boolean, arguments: AST[Any]*) extends AST[RT]

case class PatternStatsCall[RT](functionName: Symbol, inner: AST[Any], window: Window) extends AST[RT]

case class AggregateCall[RT](functionName: Symbol, value: AST[Any], window: Window) extends AST[RT]

case class AndThen(first: AST[Any], second: AST[Any]) extends AST[Any]

case class Timer(cond: AST[Any], interval: TimeInterval) extends AST[Any]

case class For(accum: AST[Boolean], range: Interval[Long], exactly: Option[Boolean]) extends AST[Any]

case class Assert(cond: AST[Boolean]) extends AST[Any]