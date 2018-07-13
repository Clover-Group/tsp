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
      ignoreCase("andthen") ~ ws ~ trileanTerm ~> ((e: Expr, f: Expr) => TrileanOperatorExpr(TrileanOperators.AndThen, e, f))
        | ignoreCase("and") ~ ws ~ trileanTerm ~> ((e: Expr, f: Expr) => TrileanOperatorExpr(TrileanOperators.And, e, f))
        | ignoreCase("or") ~ ws ~ trileanTerm ~> ((e: Expr, f: Expr) => TrileanOperatorExpr(TrileanOperators.Or, e, f))
    )
  }

  def trileanTerm: Rule1[Expr] = rule {
    (trileanFactor ~ ignoreCase("for") ~ ws ~ optional(ignoreCase("exactly") ~> (() => 1)) ~ time ~ optional(range) ~>
      ((c: Expr, ex: Option[Int], w: TimeLiteral, r: Option[Expr])
      => TrileanExpr(c, exactly = ex.isDefined, window = w, range = r.orNull))
      | trileanFactor ~ ignoreCase("until") ~ ws ~ booleanExpr ~ optional(range) ~>
      ((c: Expr, b: Expr, r: Option[Expr]) => TrileanExpr(c, until = b, range = r.orNull))
      | trileanFactor
      )
  }

  def trileanFactor: Rule1[Expr] = rule {
    booleanExpr | '(' ~ trileanExpr ~ ')' ~ ws
  }

  def booleanExpr: Rule1[Expr] = rule {
    booleanTerm ~ zeroOrMore(ignoreCase("or") ~ ws ~ booleanTerm ~> ((e: Expr, f: Expr) => BooleanOperatorExpr(BooleanOperators.Or, e, f)))
  }

  def booleanTerm: Rule1[Expr] = rule {
    booleanFactor ~ zeroOrMore(ignoreCase("and") ~ ws ~ booleanFactor ~> ((e: Expr, f: Expr) => BooleanOperatorExpr(BooleanOperators.And, e, f)))
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
    real | integer | boolean | string | functionCall | identifier | '(' ~ expr ~ ')' ~ ws
  }

  def range: Rule1[Expr] = rule {
    timeRange | repetitionRange
  }

  def timeRange: Rule1[Expr] = rule {
    ("<" ~ ws ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(null, t, strict = true))
      | "<=" ~ ws ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(null, t, strict = false))
      | ">" ~ ws ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(t, null, strict = true))
      | ">=" ~ ws ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(t, null, strict = false))
      | time ~ ignoreCase("to") ~ ws ~ time ~> ((t1: TimeLiteral, t2: TimeLiteral) => TimeRangeExpr(t1, t2, strict = false))
      | real ~ ignoreCase("to") ~ ws ~ real ~ timeUnit ~> ((d1: DoubleLiteral, d2: DoubleLiteral, u: Int) =>
      TimeRangeExpr(TimeLiteral((d1.value * u).toLong), TimeLiteral((d2.value * u).toLong), strict = false))
      )
  }

  def repetitionRange: Rule1[Expr] = rule {
    ("<" ~ ws ~ repetition ~> ((t: IntegerLiteral) => RepetitionRangeExpr(null, t, strict = true))
      | "<=" ~ ws ~ repetition ~> ((t: IntegerLiteral) => RepetitionRangeExpr(null, t, strict = false))
      | ">" ~ ws ~ repetition ~> ((t: IntegerLiteral) => RepetitionRangeExpr(t, null, strict = true))
      | ">=" ~ ws ~ repetition ~> ((t: IntegerLiteral) => RepetitionRangeExpr(t, null, strict = false))
      | integer ~ ignoreCase("to") ~ ws ~ repetition ~> ((t1: IntegerLiteral, t2: IntegerLiteral) => RepetitionRangeExpr(t1, t2, strict = false))
      )
  }

  def repetition: Rule1[IntegerLiteral] = rule {
    integer ~ ignoreCase("times")
  }

  def time: Rule1[TimeLiteral] = rule {
    singleTime.+(ws) ~> ((ts: Seq[TimeLiteral]) => TimeLiteral(ts.foldLeft(0L) { (acc, t) => acc + t.millis }))
  }

  def singleTime: Rule1[TimeLiteral] = rule {
    real ~ timeUnit ~ ws ~>
      ((i: DoubleLiteral, u: Int) => TimeLiteral((i.value * u).toLong))
  }

  def timeUnit: Rule1[Int] = rule {
    (ignoreCase("seconds") ~> (() => 1000)
      | ignoreCase("sec") ~> (() => 1000)
      | ignoreCase("minutes") ~> (() => 60000)
      | ignoreCase("min") ~> (() => 60000)
      | ignoreCase("milliseconds") ~> (() => 1)
      | ignoreCase("ms") ~> (() => 1)
      | ignoreCase("hours") ~> (() => 3600000)
      | ignoreCase("hr") ~> (() => 3600000))
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

  def functionCall: Rule1[FunctionCallExpr] = rule {
    identifier ~ ws ~ "(" ~ ws ~ (time | expr).*(ws ~ "," ~ ws) ~ ")" ~ ws ~> ((i: Identifier, el: Seq[Expr]) => FunctionCallExpr(i.identifier, el.toList))
  }

  def identifier: Rule1[Identifier] = rule {
    (capture(CharPredicate.Alpha ~ zeroOrMore(CharPredicate.AlphaNum | '_')) ~ ws ~> ((id: String) => Identifier(id))
      | '"' ~ capture(oneOrMore(noneOf("\"") | "\"\"")) ~ '"' ~ ws ~> ((id: String) => Identifier(id.replace("\"\"", "\"")))
      )
  }

  def string: Rule1[StringLiteral] = rule {
    "'" ~ capture(oneOrMore(noneOf("'") | "''")) ~ "'" ~ ws ~> ((id: String) => StringLiteral(id.replace("'", "''")))
  }

  def boolean: Rule1[BooleanLiteral] = rule {
    (ignoreCase("true") ~ ws ~> (() => BooleanLiteral(true))
      | ignoreCase("false") ~ ws ~> (() => BooleanLiteral(false)) ~ ws)
  }

  def ws = rule {
    quiet(zeroOrMore(anyOf(" \t \n")))
  }
}
