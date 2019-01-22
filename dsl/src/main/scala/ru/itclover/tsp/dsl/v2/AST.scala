package ru.itclover.tsp.dsl.v2
import ru.itclover.tsp.core.Intervals.{Interval, TimeInterval}
import ru.itclover.tsp.core.Window
import ru.itclover.tsp.utils.UtilityTypes.ParseException
import ru.itclover.tsp.v2.Result
import shapeless.HList
import shapeless.ops.hlist.Mapped

import scala.reflect.ClassTag

sealed trait AST extends Product with Serializable {
  val valueType: ASTType

  def requireType(requirementType: ASTType, message: Any): Unit =
    if(valueType != requirementType) throw ParseException(message.toString)
}

case class Constant[T](value: T)(implicit ct: ClassTag[T]) extends AST {
  override val valueType: ASTType = ASTType.of[T]
}

// TODO: Other types for columns
case class Identifier(value: Symbol) extends AST {
  override val valueType: ASTType = DoubleASTType
}

case class Range[T](from: T, to: T)(implicit ct: ClassTag[T]) extends AST {
  override val valueType: ASTType = ASTType.of[T]
}

case class FunctionCall[RT](functionName: Symbol, arguments: Seq[AST])(implicit ct: ClassTag[RT]) extends AST {
  override val valueType: ASTType = ASTType.of[RT]
}

case class ReducerFunctionCall[RT](functionName: Symbol, cond: Result[Any] => Boolean, arguments: Seq[AST])(
  implicit ct: ClassTag[RT]
) extends AST {
  override val valueType: ASTType = ASTType.of[RT]
}

case class PatternStatsCall[RT](functionName: Symbol, inner: AST, window: Window)(implicit ct: ClassTag[RT])
    extends AST {
  override val valueType: ASTType = ASTType.of[RT]
}

case class AggregateCall[RT](functionName: Symbol, value: AST, window: Window)(implicit ct: ClassTag[RT]) extends AST {
  override val valueType: ASTType = ASTType.of[RT]
}

case class AndThen(first: AST, second: AST) extends AST {
  first.requireType(BooleanASTType, s"1st argument '$first' must be boolean in '$this'")
  second.requireType(BooleanASTType, s"2nd argument '$second' must be boolean in '$this'")
  override val valueType: ASTType = BooleanASTType
}

case class Timer(cond: AST, interval: TimeInterval) extends AST {
  override val valueType: ASTType = BooleanASTType
}

case class For(accum: AST, range: Interval[Long], exactly: Option[Boolean]) extends AST {
  override val valueType: ASTType = BooleanASTType
}

case class Assert(cond: AST) extends AST {
  override val valueType: ASTType = BooleanASTType
}
