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

//final case class TrileanExpr() extends expr
final case class ComparisonOperatorExpr(loc: List[Line], op: ComparisonOperators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class OperatorExpr(loc: List[Line], op: Operators.Value, lhs: Expr, rhs: Expr) extends Expr

final case class Identifier(loc: List[Line], identifier: String) extends Expr

final case class IntegerLiteral(loc: List[Line], value: Long) extends Expr

final case class DoubleLiteral(loc: List[Line], value: Double) extends Expr

final case class StringLiteral(loc: List[Line], value: String) extends Expr

final case class BooleanLiteral(loc: List[Line], value: Boolean) extends Expr

class UserRuleParser {
  val parser = {
    implicit val W: Whitespace = Whitespace("""\s+""".r)

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

    lazy val factor: Parser[Expr] = {
      double | int | id | "(" ~ expr ~ ")"
    }

    lazy val double: Parser[Expr] = """[+-]?\d+(\.\d+)?""".r ^^ { (loc, str) => DoubleLiteral(loc, str.toDouble) }
    lazy val int: Parser[Expr] = """[+-]?\d+""".r ^^ { (loc, str) => IntegerLiteral(loc, str.toInt) }
    lazy val id: Parser[Expr] = """\w+""".r ^^ { (loc, str) => Identifier(loc, str) }

  }
}
