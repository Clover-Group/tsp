package ru.itclover.streammachine.newsyntax

import org.parboiled2._
import ru.itclover.streammachine.aggregators.AggregatorPhases.{Aligned, Skip, ToSegments}
import ru.itclover.streammachine.core.Time.{MaxWindow, TimeExtractor}
import ru.itclover.streammachine.core.{PhaseParser, Time, TimeInterval, Window}
import ru.itclover.streammachine.phases.BooleanPhases.{AndParser, BooleanPhaseParser, NotParser, OrParser, XorParser}
//import ru.itclover.streammachine.newsyntax.TrileanOperators.{And, AndThen, Or}
import ru.itclover.streammachine.phases.BooleanPhases.{Assert, ComparingParser}
import ru.itclover.streammachine.phases.CombiningPhases.TogetherParser
import ru.itclover.streammachine.phases.ConstantPhases.OneRowPhaseParser
import ru.itclover.streammachine.phases.NoState
import ru.itclover.streammachine.phases.NumericPhases._
import shapeless.HNil

import scala.collection.immutable.NumericRange

class SyntaxParser[Event](val input: ParserInput)(implicit val timeExtractor: TimeExtractor[Event],
                                                  symbolNumberExtractor: SymbolNumberExtractor[Event]) extends Parser {

  type AnyPhaseParser = PhaseParser[Event, Any, Any]
  type AnyBooleanPhaseParser = BooleanPhaseParser[Event, Any]
  type AnyNumericPhaseParser = NumericPhaseParser[Event, Any]

  def start: Rule1[AnyPhaseParser] = rule {
    trileanExpr ~ EOI ~> ((e: AnyPhaseParser) => ToSegments(e)(timeExtractor).asInstanceOf[AnyPhaseParser])
  }

  def trileanExpr: Rule1[AnyPhaseParser] = rule {
    trileanTerm ~ zeroOrMore(
      ignoreCase("andthen") ~ ws ~ trileanTerm ~>
        ((e: AnyPhaseParser, f: AnyPhaseParser) => (e andThen Skip(1, f)).asInstanceOf[AnyPhaseParser])
        | ignoreCase("and") ~ ws ~ trileanTerm ~>
        ((e: AnyPhaseParser, f: AnyPhaseParser) => (e togetherWith f).asInstanceOf[AnyPhaseParser])
        | ignoreCase("or") ~ ws ~ trileanTerm ~>
        ((e: AnyPhaseParser, f: AnyPhaseParser) => (e either f).asInstanceOf[AnyPhaseParser])
    )
  }

  def trileanTerm: Rule1[AnyPhaseParser] = rule {
    (trileanFactor ~ ignoreCase("for") ~ ws ~ optional(ignoreCase("exactly") ~ ws ~> (() => 1)) ~ time ~
      optional(range) ~> ((c: AnyPhaseParser, ex: Option[Int], w: Window, r: Option[Any]) => {
      val ac: AnyPhaseParser = r match {
        case Some(nr) if nr.isInstanceOf[NumericRange[_]] =>
          val q = PhaseParser.Functions.truthCount(c.asInstanceOf[AnyBooleanPhaseParser], w)
          Assert(q.map[Boolean](x => nr.asInstanceOf[NumericRange[Long]].contains(x))).asInstanceOf[AnyPhaseParser]
        case Some(tr) if tr.isInstanceOf[TimeInterval] =>
          val q = PhaseParser.Functions.truthMillisCount(c.asInstanceOf[AnyBooleanPhaseParser], w)
          Assert(q.map[Boolean](x => tr.asInstanceOf[TimeInterval].contains(Window(x)))).asInstanceOf[AnyPhaseParser]
        case _ => c
      }
      (if (ex.isDefined) ac.timed(w, w) else ac.timed(Time.less(w))).asInstanceOf[AnyPhaseParser]
    })
      | trileanFactor ~ ignoreCase("until") ~ ws ~ booleanExpr ~ optional(range) ~>
      ((c: AnyPhaseParser, b: AnyBooleanPhaseParser, r: Option[Any]) => {
        val ac = c
        (ac.timed(MaxWindow).asInstanceOf[AnyBooleanPhaseParser] and
          Assert(NotParser(b)).asInstanceOf[AnyBooleanPhaseParser]).asInstanceOf[AnyPhaseParser]
      })
      | trileanFactor
      )
  }

  def trileanFactor: Rule1[AnyPhaseParser] = rule {
    booleanExpr ~> { b: AnyBooleanPhaseParser => Assert(b) } | '(' ~ trileanExpr ~ ')' ~ ws
  }

  def booleanExpr: Rule1[AnyBooleanPhaseParser] = rule {
    booleanTerm ~ zeroOrMore(
      ignoreCase("or") ~ ws ~ booleanTerm ~>
        ((e: AnyBooleanPhaseParser, f: AnyBooleanPhaseParser) => OrParser(e, f).asInstanceOf[AnyBooleanPhaseParser])
    | ignoreCase("xor") ~ ws ~ booleanTerm ~>
        ((e: AnyBooleanPhaseParser, f: AnyBooleanPhaseParser) => XorParser(e, f).asInstanceOf[AnyBooleanPhaseParser]))
  }

  def booleanTerm: Rule1[AnyBooleanPhaseParser] = rule {
    booleanFactor ~ zeroOrMore(
      ignoreCase("and") ~ ws ~ booleanFactor ~>
        ((e: AnyBooleanPhaseParser, f: AnyBooleanPhaseParser) => AndParser(e, f).asInstanceOf[AnyBooleanPhaseParser]))
  }

  def booleanFactor: Rule1[AnyBooleanPhaseParser] = rule {
    comparison |
      boolean ~> ((b: OneRowPhaseParser[Event, Boolean]) => b.asInstanceOf[AnyBooleanPhaseParser]) |
      "(" ~ booleanExpr ~ ")" ~ ws | "not" ~ booleanExpr ~> ((b: AnyBooleanPhaseParser) => NotParser(b))
  }

  def comparison: Rule1[AnyBooleanPhaseParser] = rule {
    (
      expr ~ "<" ~ ws ~ expr ~> ((e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
        new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 < d2,
          "<") {}.asInstanceOf[AnyBooleanPhaseParser])
        | expr ~ "<=" ~ ws ~ expr ~> ((e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
        new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 <= d2,
          "<=") {}.asInstanceOf[AnyBooleanPhaseParser])
        | expr ~ ">" ~ ws ~ expr ~> ((e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
        new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 > d2,
          ">") {}.asInstanceOf[AnyBooleanPhaseParser])
        | expr ~ ">=" ~ ws ~ expr ~> ((e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
        new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 >= d2,
          ">") {}.asInstanceOf[AnyBooleanPhaseParser])
        | expr ~ "=" ~ ws ~ expr ~> ((e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
        new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 == d2,
          "==") {}.asInstanceOf[AnyBooleanPhaseParser])
        |
        expr ~ ("!=" | "<>") ~ ws ~ expr ~> ((e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
          new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 != d2,
            "!=") {}.asInstanceOf[AnyBooleanPhaseParser])
      )
  }

  def expr: Rule1[AnyNumericPhaseParser] = rule {
    term ~ zeroOrMore('+' ~ ws ~ term ~> ((e: AnyNumericPhaseParser, f: AnyNumericPhaseParser) =>
      new BinaryNumericParser[Event, Any, Any, Double](e, f, _ + _, "+").asInstanceOf[AnyNumericPhaseParser])
      | '-' ~ ws ~ term ~> ((e: AnyNumericPhaseParser, f: AnyNumericPhaseParser) =>
      new BinaryNumericParser[Event, Any, Any, Double](e, f, _ - _, "-").asInstanceOf[AnyNumericPhaseParser])
    )
  }

  def term: Rule1[AnyNumericPhaseParser] = rule {
    factor ~
      zeroOrMore('*' ~ ws ~ factor ~> ((e: AnyNumericPhaseParser, f: AnyNumericPhaseParser) =>
        new BinaryNumericParser[Event, Any, Any, Double](e, f, _ * _, "*").asInstanceOf[AnyNumericPhaseParser])
        | '/' ~ ws ~ factor ~> ((e: AnyNumericPhaseParser, f: AnyNumericPhaseParser) =>
        new BinaryNumericParser[Event, Any, Any, Double](e, f, _ / _, "/").asInstanceOf[AnyNumericPhaseParser])
      )
  }

  def factor: Rule1[AnyNumericPhaseParser] = rule {
    (
      real ~> (_.asInstanceOf[AnyNumericPhaseParser])
        | integer ~> (_.asInstanceOf[AnyNumericPhaseParser])
        | functionCall
        | identifier ~> (_.as[Double].asInstanceOf[AnyNumericPhaseParser])
        | '(' ~ expr ~ ')' ~ ws
      )
  }

  def underscoreConstraint: Rule1[Double => Boolean] = rule {
    underscoreConjunction ~ zeroOrMore(
      ignoreCase("or") ~ ws ~ underscoreConjunction ~>
        ((e: Double => Boolean, f: Double => Boolean) => (x: Double) => e(x) || f(x))
        | ignoreCase("xor") ~ ws ~ underscoreConjunction ~>
        ((e: Double => Boolean, f: Double => Boolean) => (x: Double) => e(x) != f(x))
    )
  }

  def underscoreConjunction: Rule1[Double => Boolean] = rule {
    underscoreCond ~ zeroOrMore(
      ignoreCase("and") ~ ws ~ underscoreCond ~>
        ((e: Double => Boolean, f: Double => Boolean) => (x: Double) => e(x) && f(x))
    )
  }

  def underscoreCond: Rule1[Double => Boolean] = rule {
    (
      underscoreComparison
        | boolean ~> ((e: OneRowPhaseParser[Event, Boolean]) => (_: Double) => e.extract(null.asInstanceOf[Event]))
        | '(' ~ underscoreConstraint ~ ')'
        | ignoreCase("not") ~ underscoreCond ~> ((e: Double => Boolean) => (x: Double) => !e(x))
      )
  }

  def underscoreComparison: Rule1[Double => Boolean] = rule {
    (
      underscoreExpr ~ "<" ~ ws ~ underscoreExpr ~>
        ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) < f(x))
        | underscoreExpr ~ "<=" ~ ws ~ underscoreExpr ~>
        ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) <= f(x))
        | underscoreExpr ~ ">" ~ ws ~ underscoreExpr ~>
        ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) > f(x))
        | underscoreExpr ~ ">=" ~ ws ~ underscoreExpr ~>
        ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) >= f(x))
        | underscoreExpr ~ "==" ~ ws ~ underscoreExpr ~>
        ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) == f(x))
        | underscoreExpr ~ ("!=" | "<>") ~ ws ~ underscoreExpr ~>
        ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) != f(x))
      )
  }

  def underscoreExpr: Rule1[Double => Double] = rule {
    underscoreTerm ~
      zeroOrMore(
        '+' ~ ws ~ underscoreTerm ~> ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) + f(x))
          | '-' ~ ws ~ underscoreTerm ~> ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) - f(x))
      )
  }

  def underscoreTerm: Rule1[Double => Double] = rule {
    underscoreFactor ~
      zeroOrMore(
        '*' ~ ws ~ underscoreFactor ~> ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) * f(x))
          | '/' ~ ws ~ underscoreFactor ~> ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) / f(x)))
  }

  def underscoreFactor: Rule1[Double => Double] = rule {
    (
      real ~ ws ~> ((r: OneRowPhaseParser[Event, Double]) => (_: Double) => r.extract(null.asInstanceOf[Event]))
        | integer ~ ws ~>
        ((r: OneRowPhaseParser[Event, Long]) => (_: Double) => r.extract(null.asInstanceOf[Event]).toDouble)
        | str("_") ~ ws ~> (() => (x: Double) => x)
        | '(' ~ underscoreExpr ~ ')' ~ ws
      )
  }

  def range: Rule1[Any] = rule {
    timeRange | repetitionRange
  }

  def timeRange: Rule1[TimeInterval] = rule {
    ("<" ~ ws ~ time ~> ((t: Window) => Time.less(t))
      | "<=" ~ ws ~ time ~> ((t: Window) => Time.less(t))
      | ">" ~ ws ~ time ~> ((t: Window) => Time.more(t))
      | ">=" ~ ws ~ time ~> ((t: Window) => Time.more(t))
      | time ~ ignoreCase("to") ~ ws ~ time ~>
      ((t1: Window, t2: Window) => TimeInterval(t1, t2))
      | real ~ ignoreCase("to") ~ ws ~ real ~ timeUnit ~>
      ((d1: OneRowPhaseParser[Event, Double], d2: OneRowPhaseParser[Event, Double], u: Int) =>
        TimeInterval(Window((d1.extract(null.asInstanceOf[Event]) * u).toLong),
          Window((d2.extract(null.asInstanceOf[Event]) * u).toLong)))
      )
  }

  def repetitionRange: Rule1[NumericRange[Long]] = rule {
    ("<" ~ ws ~ repetition ~> ((t: Long) => 0L until t)
      | "<=" ~ ws ~ repetition ~> ((t: Long) => 0L to t)
      | ">" ~ ws ~ repetition ~> ((t: Long) => t + 1 to Int.MaxValue.toLong)
      | ">=" ~ ws ~ repetition ~> ((t: Long) => t to Int.MaxValue.toLong)
      | integer ~ ignoreCase("to") ~ ws ~ repetition ~>
      ((t1: OneRowPhaseParser[Event, Long], t2: Long) => t1.extract(null.asInstanceOf[Event]) to t2)
      )
  }

  def repetition: Rule1[Long] = rule {
    integer ~ ignoreCase("times") ~> ((e: OneRowPhaseParser[Event, Long]) => e.extract(null.asInstanceOf[Event]))
  }

  def time: Rule1[Window] = rule {
    singleTime.+(ws) ~> ((ts: Seq[Window]) => Window(ts.foldLeft(0L) { (acc, t) => acc + t.toMillis }))
  }

  def singleTime: Rule1[Window] = rule {
    real ~ timeUnit ~ ws ~>
      ((i: OneRowPhaseParser[Event, Double], u: Int) => Window((i.extract(null.asInstanceOf[Event]) * u).toLong))
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

  def real: Rule1[OneRowPhaseParser[Event, Double]] = rule {
    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1)) ~
      capture(oneOrMore(CharPredicate.Digit) ~ optional('.' ~ oneOrMore(CharPredicate.Digit))) ~ ws
      ~> ((s: Int, i: String) => OneRowPhaseParser[Event, Double](_ => s * i.toDouble))
      )
  }

  def integer: Rule1[OneRowPhaseParser[Event, Long]] = rule {
    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1))
      ~ capture(oneOrMore(CharPredicate.Digit)) ~ ws
      ~> ((s: Int, i: String) => OneRowPhaseParser[Event, Long](_ => s * i.toLong))
      )
  }

  def functionCall: Rule1[AnyNumericPhaseParser] = rule {
    (
      identifier ~ ws ~ "(" ~ ws ~ expr.*(ws ~ "," ~ ws) ~ optional(";" ~ ws ~ underscoreConstraint) ~ ws ~ ")" ~ ws ~>
        ((i: SymbolParser[Event], arguments: Seq[AnyNumericPhaseParser], constraint: Option[Double => Boolean]) => {
          val function = i.symbol.toString.tail.toLowerCase
          val cond: Double => Boolean = constraint.getOrElse(_ => true)
          function match {
            case "lag" => PhaseParser.Functions.lag(arguments.head).asInstanceOf[AnyNumericPhaseParser]
            case "abs" => PhaseParser.Functions.abs(arguments.head).asInstanceOf[AnyNumericPhaseParser]
            case "minof" =>
              val as = arguments
              Reduce[Event, Any](TestFunctions.min(_, _, cond))(OneRowPhaseParser[Event, Double](_ => Double.MaxValue).asInstanceOf[AnyNumericPhaseParser], as: _*).asInstanceOf[AnyNumericPhaseParser]
            case "maxof" =>
              val as = arguments
              Reduce[Event, Any](TestFunctions.max(_, _, cond))(OneRowPhaseParser[Event, Double](_ => Double.MinValue).asInstanceOf[AnyNumericPhaseParser], as: _*).asInstanceOf[AnyNumericPhaseParser]
            case "avgof" =>
              val as = arguments
              (Reduce[Event, Any](TestFunctions.plus(_, _, cond))(OneRowPhaseParser[Event, Double](_ => 0.0).asInstanceOf[AnyNumericPhaseParser], as: _*) div
                Reduce[Event, Any](TestFunctions.countNotNan(_, _, cond))(OneRowPhaseParser[Event, Double](_ => 0.0).asInstanceOf[AnyNumericPhaseParser], as: _*)).asInstanceOf[AnyNumericPhaseParser]
            case "countof" =>
              val as = arguments
              Reduce[Event, Any](TestFunctions.countNotNan(_, _, cond))(OneRowPhaseParser[Event, Double](_ => Double.MinValue).asInstanceOf[AnyNumericPhaseParser], as: _*).asInstanceOf[AnyNumericPhaseParser]
            case _ => throw new RuntimeException(s"Unknown function `$function`")
          }
        })
        | identifier ~ ws ~ "(" ~ ws ~ expr ~ ws ~ "," ~ ws ~ time ~ ws ~ ")" ~ ws ~>
        (
          (i: SymbolParser[Event], arg: AnyNumericPhaseParser, w: Window) => {
            val function = i.symbol.toString.tail
            function match {
              case "avg" => PhaseParser.Functions.avg(arg, w).asInstanceOf[AnyNumericPhaseParser]
              case "sum" => PhaseParser.Functions.sum(arg, w).asInstanceOf[AnyNumericPhaseParser]
            }
          }
          )
      )
  }

  def identifier: Rule1[SymbolParser[Event]] = rule {
    (capture(CharPredicate.Alpha ~ zeroOrMore(CharPredicate.AlphaNum | '_')) ~ ws ~>
      ((id: String) => SymbolParser[Event](Symbol(id)))
      | '"' ~ capture(oneOrMore(noneOf("\"") | "\"\"")) ~ '"' ~ ws ~>
      ((id: String) => SymbolParser[Event](Symbol(id.replace("\"\"", "\""))))
      )
  }

  def string: Rule1[OneRowPhaseParser[Event, String]] = rule {
    "'" ~ capture(oneOrMore(noneOf("'") | "''")) ~ "'" ~ ws ~>
      ((str: String) => OneRowPhaseParser[Event, String](_ => str.replace("''", "'")))
  }

  def boolean: Rule1[OneRowPhaseParser[Event, Boolean]] = rule {
    (ignoreCase("true") ~ ws ~> (() => OneRowPhaseParser[Event, Boolean](_ => true))
      | ignoreCase("false") ~ ws ~> (() => OneRowPhaseParser[Event, Boolean](_ => false)) ~ ws)
  }

  def ws = rule {
    quiet(zeroOrMore(anyOf(" \t \n \r")))
  }
}

object TestFunctions {
  def min(d1: Double, d2: Double, cond: Double => Boolean): Double = Math.min(
    if (d1.isNaN) Double.MaxValue else d1,
    if (d2.isNaN || !cond(d2)) Double.MaxValue else d2)

  def max(d1: Double, d2: Double, cond: Double => Boolean): Double = Math.max(
    if (d1.isNaN) Double.MinValue else d1,
    if (d2.isNaN || !cond(d2)) Double.MinValue else d2)

  def plus(d1: Double, d2: Double, cond: Double => Boolean): Double = (if (d1.isNaN) 0 else d1) +
    (if (d2.isNaN || !cond(d2)) 0 else d2)

  def countNotNan(d1: Double, d2: Double, cond: Double => Boolean): Double = (if (d1.isNaN) 0 else d1) +
    (if (d2.isNaN || !cond(d2)) 0 else 1)
}