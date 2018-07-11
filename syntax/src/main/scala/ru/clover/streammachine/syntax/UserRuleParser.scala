package ru.clover.streammachine.syntax

import parseback._

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

//final case class TrileanExpr() extends expr

final case class FunctionCallExpr(loc: List[Line], fun: String, args: List[Expr]) extends Expr

final case class ComparisonOperatorExpr(loc: List[Line], op: ComparisonOperators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class BooleanOperatorExpr(loc: List[Line], op: BooleanOperators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class OperatorExpr(loc: List[Line], op: Operators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class Identifier(loc: List[Line], identifier: String) extends Expr

final case class IntegerLiteral(loc: List[Line], value: Long) extends Expr

final case class DoubleLiteral(loc: List[Line], value: Double) extends Expr

final case class StringLiteral(loc: List[Line], value: String) extends Expr

final case class BooleanLiteral(loc: List[Line], value: Boolean) extends Expr

class UserRuleParser {
  val parser: Parser[Expr] = {
    implicit val W: Whitespace = Whitespace("""\s+""".r)

    lazy val booleanExpr: Parser[Expr] = (
      comparison ^^ { (_, e) => e }
        | booleanLiteral ^^ { (_, e) => e }
        | "(" ~ booleanExpr ~ ")" ^^ { (_, _, e, _) => e }
        | booleanExpr ~ "and" ~ booleanExpr ^^ {(loc, e1, _, e2) => BooleanOperatorExpr(loc, BooleanOperators.And, e1, e2) }
        | booleanExpr ~ "or" ~ booleanExpr ^^ {(loc, e1, _, e2) => BooleanOperatorExpr(loc, BooleanOperators.Or, e1, e2) }
        | "not" ~ booleanExpr ^^ {(loc, _, e1) => BooleanOperatorExpr(loc, BooleanOperators.Not, e1, null) }
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

    lazy val double: Parser[Expr] = """[+-]?\d+(\.\d+)?""".r ^^ { (loc, str) => DoubleLiteral(loc, str.toDouble) }
    lazy val int: Parser[Expr] = """[+-]?\d+""".r ^^ { (loc, str) => IntegerLiteral(loc, str.toInt) }
    lazy val id: Parser[Identifier] = """\w+""".r ^^ { (loc, str) => Identifier(loc, str) }
    lazy val booleanLiteral: Parser[Expr] = (
      "true" ^^ { (loc, _) => BooleanLiteral(loc, value = true) }
        | "false" ^^ { (loc, _) => BooleanLiteral(loc, value = false) }

      )

    expr
  }
}
