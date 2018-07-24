package ru.itclover.streammachine.newsyntax

import ru.itclover.streammachine.aggregators.AggregatorPhases.{Aligned, Skip, ToSegments}
import ru.itclover.streammachine.core.Time.{MaxWindow, TimeExtractor}
import ru.itclover.streammachine.core.{PhaseParser, Time, Window}
import ru.itclover.streammachine.newsyntax.TrileanOperators.{And, AndThen, Or}
import ru.itclover.streammachine.phases.BooleanPhases.{Assert, BooleanPhaseParser, ComparingParser}
import ru.itclover.streammachine.phases.ConstantPhases.OneRowPhaseParser
import ru.itclover.streammachine.phases.NumericPhases.{BinaryNumericParser, SymbolNumberExtractor, SymbolParser}

class PhaseBuilder[Event] {

  def build(x: Expr)(implicit timeExtractor: TimeExtractor[Event],
                     numberExtractor: SymbolNumberExtractor[Event]): PhaseParser[Event, _, _] = {
    ToSegments(buildParser(x, maxTimePhase(x)))
  }

  protected def buildParser(x: Expr, maxPhase: Long, level: Int = 0, asAssert: Boolean = false)
                           (implicit timeExtractor: TimeExtractor[Event],
                            numberExtractor: SymbolNumberExtractor[Event]): PhaseParser[Event, _, _] = {
    def nextBuild(x: Expr, asAssert: Boolean = false) = buildParser(x, maxPhase, level + 1, asAssert)
    x match {
      case BooleanLiteral(value) => OneRowPhaseParser[Event, Boolean](_ => value)
      case IntegerLiteral(value) => OneRowPhaseParser[Event, Long](_ => value)
      case BooleanOperatorExpr(operator, lhs, rhs) =>
        val cp = new ComparingParser[Event, Any, Any, Boolean](
        nextBuild(lhs).asInstanceOf[PhaseParser[Event, Any, Boolean]],
        nextBuild(rhs).asInstanceOf[PhaseParser[Event, Any, Boolean]])(operator.comparingFunction,
        operator.operatorSymbol) {}
        if (asAssert) Assert(cp) else cp
      case ComparisonOperatorExpr(operator, lhs, rhs) =>
        val cp = new ComparingParser[Event, Any, Any, Double](
        nextBuild(lhs).asInstanceOf[PhaseParser[Event, Any, Double]],
        nextBuild(rhs).asInstanceOf[PhaseParser[Event, Any, Double]])(operator.comparingFunction,
        operator.operatorSymbol) {}
        if (asAssert) Assert(cp) else cp
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
      case Identifier(identifier) => SymbolParser(Symbol(identifier)).as[Double]
      case OperatorExpr(operator, lhs, rhs) =>
        val lhsParser = nextBuild(lhs).asInstanceOf[PhaseParser[Event, _, Double]]
        val rhsParser = nextBuild(rhs).asInstanceOf[PhaseParser[Event, _, Double]]
        BinaryNumericParser(lhsParser, rhsParser, operator.comp[Double], operator.operatorSymbol)
      case StringLiteral(value) => OneRowPhaseParser[Event, String](_ => value)
      case TrileanCondExpr(cond, exactly, window, range, until) =>
        if (until != null) {
          nextBuild(cond).timed(MaxWindow).asInstanceOf[PhaseParser[Event, _, Boolean]] and
            Assert(nextBuild(until).asInstanceOf[BooleanPhaseParser[Event, _]])
        }
        else {
          val w = Window(window.millis)
          val g = nextBuild(cond).asInstanceOf[BooleanPhaseParser[Event, _]]
          val c: PhaseParser[Event, _, _] = range match {
            case r: RepetitionRangeExpr =>
              val q = PhaseParser.Functions.truthCount(g, w)
              q > OneRowPhaseParser(_ => 1L)
              Assert(q.map[Boolean](x => r.contains(x)))
            case tr: TimeRangeExpr =>
              val q = PhaseParser.Functions.truthMillisCount(g, w)
              Assert(q.map[Boolean](x => tr.contains(x)))
            case _ => g
          }
          if (exactly) {
            c.timed(w, w)
          } else {
            c.timed(Time.less(w))
          }
        }
      case TrileanOperatorExpr(operator, lhs, rhs) =>
        operator match {
          case And => nextBuild(lhs, asAssert = true) togetherWith nextBuild(rhs, asAssert = true)
          case AndThen => nextBuild(lhs, asAssert = true) andThen Skip(1, nextBuild(rhs, asAssert = true))
          case Or => nextBuild(lhs, asAssert = true) either nextBuild(rhs, asAssert = true)
        }
      case TrileanOnlyBooleanExpr(cond) => nextBuild(cond)
      case _ => throw new RuntimeException(s"something went wrong parsing $x")
    }
  }

  protected def maxTimePhase(x: Expr): Long = x match {
    case TrileanCondExpr(cond, _, _, _, _) => maxTimePhase(cond)
    case FunctionCallExpr(_, args) => args.map(maxTimePhase).max
    case ComparisonOperatorExpr(_, lhs, rhs) => Math.max(maxTimePhase(lhs), maxTimePhase(rhs))
    case BooleanOperatorExpr(_, lhs, rhs) => Math.max(maxTimePhase(lhs), maxTimePhase(rhs))
    case TrileanOperatorExpr(_, lhs, rhs) => Math.max(maxTimePhase(lhs), maxTimePhase(rhs))
    case TrileanOnlyBooleanExpr(cond) => maxTimePhase(cond)
    case OperatorExpr(_, lhs, rhs) => Math.max(maxTimePhase(lhs), maxTimePhase(rhs))
    case TimeLiteral(millis) => millis
    case _ => 0
  }
}
