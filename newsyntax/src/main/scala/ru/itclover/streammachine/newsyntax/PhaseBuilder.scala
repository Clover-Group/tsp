package ru.itclover.streammachine.newsyntax

import ru.itclover.streammachine.core.Time.{MaxWindow, TimeExtractor}
import ru.itclover.streammachine.core.{PhaseParser, Time, Window}
import ru.itclover.streammachine.newsyntax.TrileanOperators.{And, AndThen, Or}
import ru.itclover.streammachine.phases.ConstantPhases.OneRowPhaseParser
import ru.itclover.streammachine.phases.NumericPhases.{BinaryNumericParser, SymbolExtractor, SymbolParser}
import ru.itclover.streammachine.phases.BooleanPhases.{Assert, BooleanPhaseParser, ComparingParser}

class PhaseBuilder[Event] {
  def build(x: Expr, level: Integer = 0)(implicit timeExtractor: TimeExtractor[Event]): PhaseParser[Event, _, _] = {
    val nextBuild = (x: Expr) => build(x, level + 1)
    x match {
      case BooleanLiteral(value) => OneRowPhaseParser[Event, Boolean](_ => value)
      case IntegerLiteral(value) => OneRowPhaseParser[Event, Long](_ => value)
      //case BooleanOperatorExpr(operator, lhs, rhs) => ComparingParser[Event, _, _, Boolean](nextBuild(lhs), nextBuild(rhs))(operator.comparingFunction, operator.operatorSymbol)
      //case ComparisonOperatorExpr(operator, lhs, rhs) => ComparingParser[Event, _, _, Double](nextBuild(lhs), nextBuild(rhs))(operator.comparingFunction, operator.operatorSymbol)
      case DoubleLiteral(value) => OneRowPhaseParser[Event, Double](_ => value)
      case FunctionCallExpr(function, arguments) => ???
      case Identifier(identifier) => SymbolParser(Symbol(identifier)).as[Double]((event: Event, symbol: Symbol) => 0) // TODO: Correct type
      //case OperatorExpr(operator, lhs, rhs) => BinaryNumericParser(nextBuild(lhs), nextBuild(rhs), operator.comp, operator.operatorSymbol)
      case RepetitionRangeExpr(lower, upper, strict) => ???
      case StringLiteral(value) => OneRowPhaseParser[Event, String](_ => value)
      case TimeLiteral(value) => ???
      case TimeRangeExpr(lower, upper, strict) => ???
      case TrileanExpr(cond, exactly, window, range, until) =>
        if (until != null) {
          nextBuild(cond).timed(MaxWindow) and
            Assert(nextBuild(until).asInstanceOf[BooleanPhaseParser[Event, _]])
        }
        else {
          val w = Window(window.millis)
          var c = nextBuild(cond)
          if (range.isInstanceOf[RepetitionRangeExpr]) {

          }
          if (range.isInstanceOf[TimeRangeExpr]) {

          }
          if (exactly) {
            c.timed(w, w)
          } else {
            c.timed(Time.less(w))
          }
        }
      case TrileanOperatorExpr(operator, lhs, rhs) =>
        operator match {
          case And => nextBuild(lhs) togetherWith nextBuild(rhs)
          case AndThen => nextBuild(lhs) andThen nextBuild(rhs)
          case Or => nextBuild(lhs) either nextBuild(rhs)
        }

    }
  }
}
