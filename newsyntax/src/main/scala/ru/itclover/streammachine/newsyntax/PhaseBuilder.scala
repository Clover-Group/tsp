package ru.itclover.streammachine.newsyntax

import ru.itclover.streammachine.aggregators.AggregatorPhases.AggregatorFunctions
import ru.itclover.streammachine.aggregators.{Aligned, Skip}
import ru.itclover.streammachine.core.Time.{MaxWindow, TimeExtractor}
import ru.itclover.streammachine.core.{PhaseParser, Time, Window}
import ru.itclover.streammachine.newsyntax.TrileanOperators.{And, AndThen, Or}
import ru.itclover.streammachine.phases.ConstantPhases.OneRowPhaseParser
import ru.itclover.streammachine.phases.NumericPhases.{BinaryNumericParser, SymbolExtractor, SymbolParser}
import ru.itclover.streammachine.phases.BooleanPhases.{Assert, BooleanPhaseParser, ComparingParser}

class PhaseBuilder[Event] {
  protected var maxPhase = 0L

  def build(x: Expr)(implicit timeExtractor: TimeExtractor[Event]): PhaseParser[Event, _, _] = {
    maxPhase = maxTimePhase(x)
    buildParser(x)
  }

  protected def buildParser(x: Expr, level: Integer = 0)
                           (implicit timeExtractor: TimeExtractor[Event]): PhaseParser[Event, _, _] = {
    val nextBuild = (x: Expr) => buildParser(x, level + 1)
    x match {
      case BooleanLiteral(value) => OneRowPhaseParser[Event, Boolean](_ => value)
      case IntegerLiteral(value) => OneRowPhaseParser[Event, Long](_ => value)
      case BooleanOperatorExpr(operator, lhs, rhs) => new ComparingParser[Event, Any, Any, Boolean](
        nextBuild(lhs).asInstanceOf[PhaseParser[Event, Any, Boolean]],
        nextBuild(rhs).asInstanceOf[PhaseParser[Event, Any, Boolean]])(operator.comparingFunction,
        operator.operatorSymbol) {}
      case ComparisonOperatorExpr(operator, lhs, rhs) => new ComparingParser[Event, Any, Any, Double](
        nextBuild(lhs).asInstanceOf[PhaseParser[Event, Any, Double]],
        nextBuild(rhs).asInstanceOf[PhaseParser[Event, Any, Double]])(operator.comparingFunction,
        operator.operatorSymbol) {}
      case DoubleLiteral(value) => OneRowPhaseParser[Event, Double](_ => value)
      case FunctionCallExpr(function, arguments) => function match {
        case "avg" =>
          val w = arguments(1).asInstanceOf[TimeLiteral].millis
          val align = maxPhase - w
          val p = PhaseParser.Functions.avg(nextBuild(arguments.head).asInstanceOf[PhaseParser[Event, _, Double]],
            Window(w))
          if (align > 0) Aligned(Window(align), p) else p
        case "sum" =>
          val w = arguments(1).asInstanceOf[TimeLiteral].millis
          val align = maxPhase - w
          val p = PhaseParser.Functions.sum(nextBuild(arguments.head).asInstanceOf[PhaseParser[Event, _, Double]],
            Window(w))
          if (align > 0) Aligned(Window(align), p) else p
        case "lag" => PhaseParser.Functions.lag(nextBuild(arguments.head))
        case "abs" => PhaseParser.Functions.abs(nextBuild(arguments.head).asInstanceOf[PhaseParser[Event, _, Double]])
        case _ => throw new RuntimeException(s"Unknown function $function")
      }
      case Identifier(identifier) => SymbolParser(Symbol(identifier)).as[Double](
        ev = new SymbolExtractor[Event, Double] {
          override def extract(event: Event, symbol: Symbol): Double = 0.0 // TODO: real identifier value
        }
      ) // TODO: Correct type
      case OperatorExpr(operator, lhs, rhs) =>
        val lhsParser = nextBuild(lhs).asInstanceOf[PhaseParser[Event, _, Double]]
        val rhsParser = nextBuild(rhs).asInstanceOf[PhaseParser[Event, _, Double]]
        BinaryNumericParser(lhsParser, rhsParser, operator.comp[Double], operator.operatorSymbol)
      case StringLiteral(value) => OneRowPhaseParser[Event, String](_ => value)
      case TrileanExpr(cond, exactly, window, range, until) =>
        if (until != null) {
          nextBuild(cond).timed(MaxWindow).asInstanceOf[PhaseParser[Event, _, Boolean]] and
            Assert(nextBuild(until).asInstanceOf[BooleanPhaseParser[Event, _]])
        }
        else {
          val w = Window(window.millis)
          val c = range match {
            case r: RepetitionRangeExpr =>
              val q = PhaseParser.Functions.truthCount(nextBuild(cond).asInstanceOf[BooleanPhaseParser[Event, _]], w)
              q.map(x => r.contains(x))
            case tr: TimeRangeExpr =>
              // TODO: truthMillisCount
              val q = PhaseParser.Functions.truthMillisCount(nextBuild(cond).asInstanceOf[BooleanPhaseParser[Event, _]],
                w)
              q.map(x => tr.contains(x))
            case _ => nextBuild(cond)
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
          case AndThen => nextBuild(lhs) andThen Skip(1, nextBuild(rhs))
          case Or => nextBuild(lhs) either nextBuild(rhs)
        }
      case _ => throw new RuntimeException(s"something went wrong parsing $x")
    }
  }

  protected def maxTimePhase(x: Expr): Long = x match {
    case TrileanExpr(cond, exactly, window, range, until) => maxTimePhase(cond)
    case FunctionCallExpr(fun, args) => args.map(maxTimePhase).max
    case ComparisonOperatorExpr(op, lhs, rhs) => Math.max(maxTimePhase(lhs), maxTimePhase(rhs))
    case BooleanOperatorExpr(op, lhs, rhs) => Math.max(maxTimePhase(lhs), maxTimePhase(rhs))
    case TrileanOperatorExpr(op, lhs, rhs) => Math.max(maxTimePhase(lhs), maxTimePhase(rhs))
    case OperatorExpr(op, lhs, rhs) => Math.max(maxTimePhase(lhs), maxTimePhase(rhs))
    case TimeRangeExpr(lower, upper, strict) => 0
    case RepetitionRangeExpr(lower, upper, strict) => 0
    case Identifier(identifier) => 0
    case IntegerLiteral(value) => 0
    case TimeLiteral(millis) => millis
    case DoubleLiteral(value) => 0
    case StringLiteral(value) => 0
    case BooleanLiteral(value) => 0
  }
}