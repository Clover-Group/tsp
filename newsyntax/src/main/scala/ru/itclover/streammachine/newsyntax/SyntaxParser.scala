package ru.itclover.streammachine.newsyntax

import org.parboiled2._
import shapeless.HNil

sealed trait Expr

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
final case class TrileanExpr(cond: Expr, exactly: Boolean = false,
                             window: TimeLiteral = null, range: Expr = null, until: Expr = null) extends Expr

final case class FunctionCallExpr(fun: String, args: List[Expr]) extends Expr

final case class ComparisonOperatorExpr(op: ComparisonOperators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class BooleanOperatorExpr(op: BooleanOperators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class TrileanOperatorExpr(op: TrileanOperators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class OperatorExpr(op: Operators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class TimeRangeExpr(lower: TimeLiteral, upper: TimeLiteral, strict: Boolean) extends Expr

final case class RepetitionRangeExpr(lower: IntegerLiteral, upper: IntegerLiteral, strict: Boolean) extends Expr

final case class Identifier(identifier: String) extends Expr

final case class IntegerLiteral(value: Long) extends Expr

final case class TimeLiteral(millis: Long) extends Expr

final case class DoubleLiteral(value: Double) extends Expr

final case class StringLiteral(value: String) extends Expr

final case class BooleanLiteral(value: Boolean) extends Expr


class SyntaxParser(val input: ParserInput) extends Parser {

  def start = rule {
    trileanExpr ~ EOI
  }

  def trileanExpr: Rule1[Expr] = rule {
    trileanTerm ~ zeroOrMore(
      "andthen" ~ ws ~ trileanTerm ~> ((e: Expr, f: Expr) => TrileanOperatorExpr(TrileanOperators.AndThen, e, f))
        | "and" ~ ws ~ trileanTerm ~> ((e: Expr, f: Expr) => TrileanOperatorExpr(TrileanOperators.And, e, f))
        | "or" ~ ws ~ trileanTerm ~> ((e: Expr, f: Expr) => TrileanOperatorExpr(TrileanOperators.Or, e, f))
    )
  }

  def trileanTerm: Rule1[Expr] = rule {
     (trileanFactor ~ "for" ~ ws ~ optional(atomic("exactly") ~> (() => 1)) ~ time ~ optional(range) ~>
        ((c: Expr, ex: Option[Int], w: TimeLiteral, r: Option[Expr])
    => TrileanExpr(c, exactly = ex.isDefined, window = w, range = r.orNull))
    | trileanFactor ~ "until" ~ ws ~ booleanExpr ~ optional(range) ~>
      ((c: Expr, b: Expr, r: Option[Expr]) => TrileanExpr(c, until = b, range = r.orNull))
    )
  }

  def trileanFactor: Rule1[Expr] = rule {
    booleanExpr | '(' ~ trileanExpr ~ ')'
  }

  def booleanExpr: Rule1[Expr] = rule {
    booleanTerm ~ zeroOrMore("or" ~ ws ~ booleanTerm ~> ((e: Expr, f: Expr) => BooleanOperatorExpr(BooleanOperators.Or, e, f)))
  }

  def booleanTerm: Rule1[Expr] = rule {
    booleanFactor ~ zeroOrMore("and" ~ ws ~ booleanFactor ~> ((e: Expr, f: Expr) => BooleanOperatorExpr(BooleanOperators.And, e, f)))
  }

  def booleanFactor: Rule1[Expr] = rule {
    (comparison | boolean | "(" ~ booleanExpr ~ ")" ~ ws
      | "not" ~ booleanExpr ~> ((b: Expr) => BooleanOperatorExpr(BooleanOperators.Not, b, null)))
  }

  def comparison: Rule1[Expr] = rule {
    (
      expr ~ "<" ~ ws ~ expr ~> ((e1: Expr, e2: Expr) =>
        ComparisonOperatorExpr(ComparisonOperators.Less, e1, e2))
        | expr ~ "<=" ~ ws ~ expr ~> ((e1: Expr, e2: Expr) =>
        ComparisonOperatorExpr(ComparisonOperators.LessOrEqual, e1, e2))
        | expr ~ ">" ~ ws ~ expr ~> ((e1: Expr, e2: Expr) =>
        ComparisonOperatorExpr(ComparisonOperators.Greater, e1, e2))
        | expr ~ ">=" ~ ws ~ expr ~> ((e1: Expr, e2: Expr) =>
        ComparisonOperatorExpr(ComparisonOperators.GreaterOrEqual, e1, e2))
        | expr ~ "=" ~ ws ~ expr ~> ((e1: Expr, e2: Expr) =>
        ComparisonOperatorExpr(ComparisonOperators.Equal, e1, e2))
        | expr ~ ("!=" | "<>") ~ ws ~ expr ~> ((e1: Expr, e2: Expr) =>
        ComparisonOperatorExpr(ComparisonOperators.NotEqual, e1, e2))
      )
  }

  def expr: Rule1[Expr] = rule {
    term ~ zeroOrMore('+' ~ ws ~ term ~> ((e: Expr, f: Expr) => OperatorExpr(Operators.Add, e, f))
      | '-' ~ ws ~ term ~> ((e: Expr, f: Expr) => OperatorExpr(Operators.Sub, e, f))
    )
  }

  def term: Rule1[Expr] = rule {
    factor ~ zeroOrMore('*' ~ ws ~ factor ~> ((e: Expr, f: Expr) => OperatorExpr(Operators.Mul, e, f))
      | '/' ~ ws ~ factor ~> ((e: Expr, f: Expr) => OperatorExpr(Operators.Div, e, f))
    )
  }

  def factor: Rule1[Expr] = rule {
    real | integer | boolean | string | identifier | '(' ~ expr ~ ')' ~ ws
  }

  def range: Rule1[Expr] = rule {
    timeRange
  }

  def timeRange: Rule1[Expr] = rule {
    ("<" ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(null, t, strict = true))
      | "<=" ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(null, t, strict = false))
      | ">" ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(t, null, strict = true))
      | ">=" ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(t, null, strict = false))
      | time ~ "to" ~ time ~> ((t1: TimeLiteral, t2: TimeLiteral) => TimeRangeExpr(t1, t2, strict = false))
      | real ~ "to" ~ real ~ timeUnit ~> ((d1: Double, d2: Double, u: Int) =>
      TimeRangeExpr(TimeLiteral((d1 * u).toLong), TimeLiteral((d2 * u).toLong), strict = false))
      )
  }

  def time: Rule1[TimeLiteral] = rule {
    real ~ timeUnit ~ ws ~>
      ((i: DoubleLiteral, u: Int) => TimeLiteral((i.value * u).toLong))
  }

  def timeUnit: Rule1[Int] = rule {
    (str("seconds") ~> (() => 1000)
      | str("sec") ~> (() => 1000)
      | str("minutes") ~> (() => 60000)
      | str("min") ~> (() => 60000)
      | str("milliseconds") ~> (() => 1)
      | str("ms") ~> (() => 1)
      | str("hours") ~> (() => 3600000)
      | str("hr") ~> (() => 3600000))
  }

  def real: Rule1[DoubleLiteral] = rule {
    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1)) ~
      capture(oneOrMore(CharPredicate.Digit) ~ optional('.' ~ oneOrMore(CharPredicate.Digit))) ~ ws
      ~> ((s: Int, i: String) => DoubleLiteral(s * i.toDouble))
      )
  }

  def integer: Rule1[IntegerLiteral] = rule {
    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1))
      ~ capture(oneOrMore(CharPredicate.Digit)) ~ ws
      ~> ((s: Int, i: String) => IntegerLiteral(s * i.toInt))
      )
  }

  def identifier: Rule1[Identifier] = rule {
    (capture(CharPredicate.Alpha ~ zeroOrMore(CharPredicate.AlphaNum | '_')) ~ ws ~> ((id: String) => Identifier(id))
      | '"' ~ capture(oneOrMore(noneOf("\"") | "\"\"")) ~ '"' ~ ws ~> ((id: String) => Identifier(id.replace("\"\"", "\"")))
      )
  }

  def string: Rule1[StringLiteral] = rule {
    "'" ~ capture(oneOrMore(noneOf("'") | "''")) ~ "'" ~> ((id: String) => StringLiteral(id.replace("'", "''")))
  }

  def boolean: Rule1[BooleanLiteral] = rule {
    (atomic("true") ~> (() => BooleanLiteral(true))
      | atomic("false") ~> (() => BooleanLiteral(false)) ~ ws)
  }

  def ws = rule {
    quiet(zeroOrMore(anyOf(" \t \n")))
  }
}
