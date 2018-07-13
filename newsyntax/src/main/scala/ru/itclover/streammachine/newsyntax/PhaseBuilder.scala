package ru.itclover.streammachine.newsyntax

import ru.itclover.streammachine.core.PhaseParser

class PhaseBuilder {
  def build(x: Expr): PhaseParser[_, _, _] = {
    x match {
      case BooleanLiteral(value) => ???
      case IntegerLiteral(value) => ???
      case BooleanOperatorExpr(_, _, _) => ???
      case ComparisonOperatorExpr(_, _, _) => ???
      case DoubleLiteral(_) => ???
      case FunctionCallExpr(_, _) => ???
      case Identifier(_) => ???
      case OperatorExpr(_, _, _) => ???
      case RepetitionRangeExpr(_, _, _) => ???
      case StringLiteral(_) => ???
      case TimeLiteral(_) => ???
      case TimeRangeExpr(_, _, _) => ???
      case TrileanExpr(_, _, _, _, _) => ???
      case TrileanOperatorExpr(_, _, _) => ???

    }
  }
}
