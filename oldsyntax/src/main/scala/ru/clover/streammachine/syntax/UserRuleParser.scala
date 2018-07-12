package ru.clover.streammachine.syntax

import cats.Eval
import parseback._
import parseback.util.Catenable
import shims.syntax.either

sealed trait Expr {
  def loc: List[Line]
}

object Operators {

  sealed trait Value

  case object Add extends Value

  case object Sub extends Value

  case object Mul extends Value

  case object Div extends Value

}

object ComparisonOperators {

  sealed trait Value

  case object Equal extends Value

  case object NotEqual extends Value

  case object Less extends Value

  case object Greater extends Value

  case object LessOrEqual extends Value

  case object GreaterOrEqual extends Value

}

object BooleanOperators {

  sealed trait Value

  case object And extends Value

  case object Or extends Value

  case object Not extends Value

}

object TrileanOperators {

  sealed trait Value

  case object And extends Value

  case object AndThen extends Value

  case object Or extends Value

}

// TODO: storing time-related attributes
final case class TrileanExpr(loc: List[Line], cond: Expr, exactly: Boolean = false,
                             window: TimeLiteral = null, range: Expr = null, until: Expr = null) extends Expr

final case class FunctionCallExpr(loc: List[Line], fun: String, args: List[Expr]) extends Expr

final case class ComparisonOperatorExpr(loc: List[Line], op: ComparisonOperators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class BooleanOperatorExpr(loc: List[Line], op: BooleanOperators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class TrileanOperatorExpr(loc: List[Line], op: TrileanOperators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class OperatorExpr(loc: List[Line], op: Operators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class TimeRangeExpr(loc: List[Line], lower: TimeLiteral, upper: TimeLiteral, strict: Boolean) extends Expr

final case class RepetitionRangeExpr(loc: List[Line], lower: IntegerLiteral, upper: IntegerLiteral, strict: Boolean) extends Expr

final case class Identifier(loc: List[Line], identifier: String) extends Expr

final case class IntegerLiteral(loc: List[Line], value: Long) extends Expr

final case class TimeLiteral(loc: List[Line], millis: Long) extends Expr

final case class DoubleLiteral(loc: List[Line], value: Double) extends Expr

final case class StringLiteral(loc: List[Line], value: String) extends Expr

final case class BooleanLiteral(loc: List[Line], value: Boolean) extends Expr

class UserRuleParser {
  implicit val W: Whitespace = Whitespace("""\s+""".r)

  val parser: Parser[Expr] = {

    lazy val trileanExpr: Parser[Expr] = (
      booleanExpr
        | trileanExpr ~ "for" ~ "(exactly)?".r ~ time ^^ {
        (loc, e, _, ex, t) =>
          TrileanExpr(loc, e, exactly = ex == "exactly", window = t)
      }
        | trileanExpr ~ "for" ~ "(exactly)?".r ~ time ~ range ^^ {
        (loc, e, _, ex, t, r) =>
          TrileanExpr(loc, e, exactly = ex == "exactly", window = t, range = r)
      }
        | trileanExpr ~ "until" ~ booleanExpr ^^ { (loc, e, _, u) => TrileanExpr(loc, e, until = u) }
        | trileanExpr ~ "until" ~ booleanExpr ~ range ^^ { (loc, e, _, u, r) => TrileanExpr(loc, e, until = u, range = r) }
        | trileanExpr ~ "andthen" ~ trileanExpr ^^ { (loc, e1, _, e2) => TrileanOperatorExpr(loc, TrileanOperators.AndThen, e1, e2) }
        | trileanExpr ~ "and" ~ trileanExpr ^^ { (loc, e1, _, e2) => TrileanOperatorExpr(loc, TrileanOperators.And, e1, e2) }
        | trileanExpr ~ "or" ~ trileanExpr ^^ { (loc, e1, _, e2) => TrileanOperatorExpr(loc, TrileanOperators.Or, e1, e2) }
        | "(" ~ trileanExpr ~ ")" ^^ { (loc, _, e, _) => e }
      )

    lazy val booleanExpr: Parser[Expr] = (
      comparison ^^ { (_, e) => e }
        | booleanLiteral ^^ { (_, e) => e }
        | "(" ~ booleanExpr ~ ")" ^^ { (_, _, e, _) => e }
        | booleanExpr ~ "and" ~ booleanExpr ^^ { (loc, e1, _, e2) => BooleanOperatorExpr(loc, BooleanOperators.And, e1, e2) }
        | booleanExpr ~ "or" ~ booleanExpr ^^ { (loc, e1, _, e2) => BooleanOperatorExpr(loc, BooleanOperators.Or, e1, e2) }
        | "not" ~ booleanExpr ^^ { (loc, _, e1) => BooleanOperatorExpr(loc, BooleanOperators.Not, e1, null) }
      )

    lazy val comparison: Parser[Expr] = (
      expr ~ "<" ~ expr ^^ { (loc, e1, _, e2) => ComparisonOperatorExpr(loc, ComparisonOperators.Less, e1, e2) }
        | expr ~ "<=" ~ expr ^^ { (loc, e1, _, e2) => ComparisonOperatorExpr(loc, ComparisonOperators.LessOrEqual, e1, e2) }
        | expr ~ ">" ~ expr ^^ { (loc, e1, _, e2) => ComparisonOperatorExpr(loc, ComparisonOperators.GreaterOrEqual, e1, e2) }
        | expr ~ ">=" ~ expr ^^ { (loc, e1, _, e2) => ComparisonOperatorExpr(loc, ComparisonOperators.GreaterOrEqual, e1, e2) }
        | expr ~ "=" ~ expr ^^ { (loc, e1, _, e2) => ComparisonOperatorExpr(loc, ComparisonOperators.Equal, e1, e2) }
        | expr ~ "!=" ~ expr ^^ { (loc, e1, _, e2) => ComparisonOperatorExpr(loc, ComparisonOperators.NotEqual, e1, e2) }
      )

    lazy val expr: Parser[Expr] = (
      expr ~ "+" ~ term ^^ { (loc, e, _, t) => OperatorExpr(loc, Operators.Add, e, t) }
        | expr ~ "-" ~ term ^^ { (loc, e, _, t) => OperatorExpr(loc, Operators.Sub, e, t) }
        | term
      )

    lazy val term: Parser[Expr] = (
      term ~ "*" ~ factor ^^ { (loc, e, _, f) => OperatorExpr(loc, Operators.Mul, e, f) }
        | term ~ "/" ~ factor ^^ { (loc, e, _, f) => OperatorExpr(loc, Operators.Div, e, f) }
        | factor
      )

    lazy val factor: Parser[Expr] = (
      double
        | int
        | id
        | "(" ~ expr ~ ")" ^^ { (_, _, e, _) => e }
        | id ~ "(" ~ exprList ~ ")" ^^ { (loc, fun, _, el, _) => FunctionCallExpr(loc, fun.identifier, el) }
      )

    lazy val exprList: Parser[List[Expr]] = (
      expr ^^ { (_, e) => List(e) }
        | expr ~ "," ~ exprList ^^ { (_, e, _, el) => e :: el }
      )

    lazy val range: Parser[Expr] = timeRange | repetitionRange

    lazy val timeRange: Parser[Expr] = (
      "<" ~ time ^^ {
        (loc, _, t) =>
          TimeRangeExpr(loc, null, t, strict = true)
      }
        | "<=" ~ time ^^ {
        (loc, _, t) =>
          TimeRangeExpr(loc, null, t, strict = false)
      } | ">" ~ time ^^ {
        (loc, _, t) =>
          TimeRangeExpr(loc, t, null, strict = true)
      } | ">=" ~ time ^^ {
        (loc, _, t) =>
          TimeRangeExpr(loc, t, null, strict = false)
      }
        | time ~ "to" ~ time ^^ {
        (loc, t1, _, t2) =>
          TimeRangeExpr(loc, t1, t2, strict = false)
      }
        | double ~ "to" ~ time ^^ {
        ???
      }
      )

    lazy val repetitionRange: Parser[Expr] = (
      int ~ "times" ^^ {
        (loc, n, _) => RepetitionRangeExpr(loc, n, n, strict = false)
      }
        | int ~ "to" ~ int ~ "times" ^^ {
        (loc, n1, _, n2, _) => RepetitionRangeExpr(loc, n1, n2, strict = false)
      }
        | "<" ~ int ~ "times" ^^ {
        (loc, _, n, _) => RepetitionRangeExpr(loc, null, n, strict = true)
      }
        | "<=" ~ int ~ "times" ^^ {
        (loc, _, n, _) => RepetitionRangeExpr(loc, null, n, strict = false)
      }
        | ">" ~ int ~ "times" ^^ {
        (loc, _, n, _) => RepetitionRangeExpr(loc, n, null, strict = true)
      }
        | ">=" ~ int ~ "times" ^^ {
        (loc, _, n, _) => RepetitionRangeExpr(loc, n, null, strict = false)
      }
      )

    lazy val time: Parser[TimeLiteral] = double ~ "milliseconds|ms|seconds|sec|minutes|min|hours|hr" ^^ {
      (loc, v, u) =>
        val coeff: Double = u match {
          case "milliseconds" => 1
          case "ms" => 1
          case "seconds" => 1000
          case "sec" => 1000
          case "minutes" => 60000
          case "min" => 60000
          case "hours" => 3600000
          case "hr" => 3600000
        }
        val t: Long = (v.value * coeff).toLong
        TimeLiteral(loc, t)
    }

    lazy val double: Parser[DoubleLiteral] = """[+-]?\d+(\.\d+)?""".r ^^ { (loc, str) => DoubleLiteral(loc, str.toDouble) }
    lazy val int: Parser[IntegerLiteral] = """[+-]?\d+""".r ^^ { (loc, str) => IntegerLiteral(loc, str.toInt) }
    lazy val id: Parser[Identifier] = """\w+""".r ^^ { (loc, str) => Identifier(loc, str) }
    lazy val booleanLiteral: Parser[Expr] = (
      "true" ^^ { (loc, _) => BooleanLiteral(loc, value = true) }
        | "false" ^^ { (loc, _) => BooleanLiteral(loc, value = false) }
      )

    expr
  }

  def parse(input: String): either.\/[List[ParseError], Catenable[Expr]] = {
    val stream: LineStream[Eval] = LineStream[Eval](input)
    parser(stream).value
  }
}
