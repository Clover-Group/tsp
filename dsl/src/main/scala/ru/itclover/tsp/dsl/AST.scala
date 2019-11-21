package ru.itclover.tsp.dsl

import cats.implicits._
import ru.itclover.tsp.core.Intervals.{Interval, TimeInterval}
import ru.itclover.tsp.core.{Result, Window}
import ru.itclover.tsp.dsl.PatternMetadataInstances.monoid
import UtilityTypes.ParseException

import scala.reflect.ClassTag

sealed trait AST extends Product with Serializable {
  val valueType: ASTType

  def metadata: PatternMetadata

  def requireType(requirementType: ASTType, message: Any): Unit =
    if (valueType != requirementType) throw ParseException(message.toString)
}

case class Constant[T](value: T)(implicit ct: ClassTag[T]) extends AST {
  def metadata: PatternMetadata = PatternMetadata.empty

  override val valueType: ASTType = ASTType.of[T]
}

case class Identifier(value: Symbol, tag: ClassTag[_]) extends AST {
  override def metadata = PatternMetadata(Set(value), 0L)

  override val valueType: ASTType = ASTType.of(tag)
}

case class Range[T](from: T, to: T)(implicit ct: ClassTag[T]) extends AST {
  def metadata: PatternMetadata = PatternMetadata.empty

  override val valueType: ASTType = ASTType.of[T]
}

// TODO@trolley Rm with Function1, Function2, Function3 - boilerplate is better than mutable maps and extra complexity
case class FunctionCall(functionName: Symbol, arguments: Seq[AST])(implicit fr: FunctionRegistry) extends AST {
  override def metadata = arguments.map(_.metadata).reduce(_ |+| _)
  override val valueType: ASTType = fr.functions.get((functionName, arguments.map(_.valueType))) match {
    case Some((_, t)) => t
    case None =>
      throw ParseException(
        s"No function with name $functionName " +
        s"and types (${arguments.map(_.valueType).mkString(", ")})"
      )
  }
}

case class ReducerFunctionCall(functionName: Symbol, @transient cond: Result[Any] => Boolean, arguments: Seq[AST])(
  implicit fr: FunctionRegistry
) extends AST {
  // require the same type for all arguments
  arguments.zipWithIndex.foreach {
    case (a, idx) =>
      a.requireType(
        arguments(0).valueType,
        s"Arguments must have the same type, but arg #1 is ${arguments(0).valueType} " +
        s"and arg #${idx + 1} is ${a.valueType}"
      )
  }

  override def metadata = arguments.map(_.metadata).reduce(_ |+| _)
  override val valueType: ASTType = fr.reducers.get((functionName, arguments(0).valueType)) match {
    case Some((_, t, _, _)) => t
    case None =>
      throw ParseException(
        s"No reducer with name $functionName " +
        s"and type ${arguments(0).valueType}"
      )
  }
}

case class AndThen(first: AST, second: AST) extends AST {
  first.requireType(BooleanASTType, s"1st argument '$first' must be boolean in '$this'")
  second.requireType(BooleanASTType, s"2nd argument '$second' must be boolean in '$this'")
  override def metadata = first.metadata |+| second.metadata

  override val valueType: ASTType = BooleanASTType
}

case class Timer(cond: AST, interval: TimeInterval, gap: Option[Window] = None) extends AST {
  // Careful! Could be wrong, depending on the PatternMetadata.sumWindowsMs use-cases
  override def metadata = cond.metadata |+| PatternMetadata(Set.empty, gap.map(_.toMillis).getOrElse(interval.max))

  override val valueType: ASTType = BooleanASTType
}

case class Skip(cond: AST, window: Window, gap: Option[Window] = None) extends AST {
  // Careful! Could be wrong, depending on the PatternMetadata.sumWindowsMs use-cases
  override def metadata = cond.metadata |+| PatternMetadata(Set.empty, gap.map(_.toMillis).getOrElse(window.toMillis))

  override val valueType: ASTType = cond.valueType
}

case class Assert(cond: AST) extends AST {
  override def metadata = cond.metadata

  override val valueType: ASTType = BooleanASTType
}

/**
  * Term for syntax like `X for [exactly] T TIME > 3 times`, where
  * @param inner is `X`
  * @param window is `T TIME`
  * @param interval is `> 3 times`
  * @param exactly special term to mark the for-expr as non-sort-circuiting
  *                (ie run to the end, even if result is obvious).
  */
case class ForWithInterval(inner: AST, exactly: Option[Boolean], window: Window, interval: Interval[Long]) extends AST {
  override def metadata = inner.metadata |+| PatternMetadata(Set.empty, window.toMillis)
  override val valueType = BooleanASTType
}

case class AggregateCall(function: AggregateFn, value: AST, window: Window, gap: Option[Window] = None) extends AST {
  override def metadata = value.metadata |+| PatternMetadata(Set.empty, gap.getOrElse(window).toMillis)

  override val valueType: ASTType = DoubleASTType //TODO: Customize return type
}

sealed trait AggregateFn extends Product with Serializable
case object Sum extends AggregateFn
case object Count extends AggregateFn
case object Avg extends AggregateFn
case object Lag extends AggregateFn

object AggregateFn {

  def fromSymbol(name: Symbol): AggregateFn = name match {
    case 'sum   => Sum
    case 'count => Count
    case 'avg   => Avg
    case 'lag   => Lag
    case _      => throw new ParseException(Seq(s"Unknown aggregator '$name'"))
  }
}

case class Cast(inner: AST, to: ASTType) extends AST {
  override val valueType: ASTType = to
  override def metadata: PatternMetadata = inner.metadata
}
