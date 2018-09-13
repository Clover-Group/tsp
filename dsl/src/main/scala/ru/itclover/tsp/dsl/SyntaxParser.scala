package ru.itclover.tsp.dsl

import org.parboiled2._
import ru.itclover.tsp.aggregators.AggregatorPhases.{Skip, ToSegments}
import ru.itclover.tsp.aggregators.accums.{AccumPhase, PushDownAccumInterval}
import ru.itclover.tsp.core.Time.{MaxWindow, TimeExtractor}
import ru.itclover.tsp.core.{Pattern, Time, Window}
import ru.itclover.tsp.core.Intervals.{Interval, NumericInterval, TimeInterval}
import ru.itclover.tsp.phases.BooleanPhases.{AndParser, BooleanPhaseParser, NotParser, OrParser, XorParser}
import ru.itclover.tsp.phases.ConstantPhases.FailurePattern
import ru.itclover.tsp.utils.UtilityTypes.ParseException
import ru.itclover.tsp.phases.BooleanPhases.{Assert, ComparingParser}
import ru.itclover.tsp.phases.ConstantPhases.OneRowPattern
import ru.itclover.tsp.phases.NumericPhases._


class SyntaxParser[Event](val input: ParserInput)(
  implicit val timeExtractor: TimeExtractor[Event],
  symbolNumberExtractor: SymbolNumberExtractor[Event]
) extends Parser {

  type AnyPhaseParser = Pattern[Event, Any, Any]
  type AnyBooleanPhaseParser = BooleanPhaseParser[Event, Any]
  type AnyNumericPhaseParser = NumericPhaseParser[Event, Any]

  val nullEvent: Event = null.asInstanceOf[Event]

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
    // Exactly is default and ignored for now
    (nonFatalTrileanFactor ~ ignoreCase("for") ~ ws ~ optional(ignoreCase("exactly") ~ ws ~> (() => 1)) ~ time ~
    optional(range) ~ ws ~> (buildForExpr(_, _, _, _))
    | trileanFactor ~ ignoreCase("until") ~ ws ~ booleanExpr ~ optional(range) ~ ws ~>
    ((c: AnyPhaseParser, b: AnyBooleanPhaseParser, r: Option[Any]) => {
      (c.timed(MaxWindow).asInstanceOf[AnyBooleanPhaseParser] and
      Assert(NotParser(b)).asInstanceOf[AnyBooleanPhaseParser]).asInstanceOf[AnyPhaseParser]
    })
    | trileanFactor)
  }

  protected def buildForExpr(phase: AnyPhaseParser, exactly: Option[Int], w: Window, range: Option[Any]) = {
    range match {
      case Some(countInterval) if countInterval.isInstanceOf[NumericInterval[Long]] => {
        val accum = Pattern.Functions.truthCount(phase.asInstanceOf[AnyBooleanPhaseParser], w)
        PushDownAccumInterval(accum, countInterval.asInstanceOf[NumericInterval[Long]])
          .flatMap({ truthCount =>
            if (countInterval.asInstanceOf[NumericInterval[Long]].contains(truthCount)) {
              OneRowPattern((_: Event) => truthCount)
            } else {
              FailurePattern(s"Interval ($countInterval) not fully accumulated ($truthCount)")
            }
          })
        .asInstanceOf[AnyPhaseParser]
      }

      case Some(timeRange) if timeRange.isInstanceOf[TimeInterval] => {
        val accum = Pattern.Functions.truthMillisCount(phase.asInstanceOf[AnyBooleanPhaseParser], w)
          .asInstanceOf[AccumPhase[Event, Any, Boolean, Long]] // TODO Covariant out
        PushDownAccumInterval[Event, Any, Boolean, Long](accum, timeRange.asInstanceOf[Interval[Long]])
          .flatMap(msCount => {
            if (timeRange.asInstanceOf[TimeInterval].contains(msCount)) {
              OneRowPattern((_: Event) => msCount)
            } else {
              FailurePattern(s"Window ($timeRange) not fully accumulated ($msCount)")
            }
          })
          .asInstanceOf[AnyPhaseParser]
      }

      case None => Assert(phase.asInstanceOf[AnyBooleanPhaseParser]).timed(w, w).asInstanceOf[AnyPhaseParser]

      case _ => throw ParseException(s"Unknown range type in `for` expr: `$range`")
    }
  }

  protected def wrapPhaseIntoPredicatePusher[InnerState, AccumOut, Out](
    phase: AccumPhase[Event, InnerState, AccumOut, Out],
    condition: AccumState[AccumOut] => Boolean,
    hasUpperBound: Boolean
  ): AggregatorPhases[Event, (InnerState, AccumState[AccumOut]), Out] = {
    if (hasUpperBound) {
      PushFalseToFailure(phase, condition)
    } else {
      PushTrueToSuccess(phase, condition)
    }
  }

  // format: off

  def nonFatalTrileanFactor: Rule1[AnyPhaseParser] = rule {
    booleanExpr ~> { b: AnyBooleanPhaseParser => b } | '(' ~ trileanExpr ~ ')' ~ ws
  }

  def trileanFactor: Rule1[AnyPhaseParser] = rule {
    booleanExpr ~> { b: AnyBooleanPhaseParser => Assert(b) } | '(' ~ trileanExpr ~ ')' ~ ws
  }

  def booleanExpr: Rule1[AnyBooleanPhaseParser] = rule {
    booleanTerm ~ zeroOrMore(
      ignoreCase("or") ~ ws ~ booleanTerm ~>
      ((e: AnyBooleanPhaseParser, f: AnyBooleanPhaseParser) => OrParser(e, f).asInstanceOf[AnyBooleanPhaseParser])
      | ignoreCase("xor") ~ ws ~ booleanTerm ~>
        ((e: AnyBooleanPhaseParser, f: AnyBooleanPhaseParser) => XorParser(e, f).asInstanceOf[AnyBooleanPhaseParser])
    )
  }

  def booleanTerm: Rule1[AnyBooleanPhaseParser] = rule {
    booleanFactor ~ zeroOrMore(
      ignoreCase("and") ~ ws ~ booleanFactor ~>
      ((e: AnyBooleanPhaseParser, f: AnyBooleanPhaseParser) => AndParser(e, f).asInstanceOf[AnyBooleanPhaseParser])
    )
  }

  def booleanFactor: Rule1[AnyBooleanPhaseParser] = rule {
    comparison |
      boolean ~> ((b: OneRowPattern[Event, Boolean]) => b.asInstanceOf[AnyBooleanPhaseParser]) |
      "(" ~ booleanExpr ~ ")" ~ ws | "not" ~ booleanExpr ~> ((b: AnyBooleanPhaseParser) => NotParser(b))
  }

  def comparison: Rule1[AnyBooleanPhaseParser] = rule {
    (
      expr ~ "<" ~ ws ~ expr ~> (
        (e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
          new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 < d2, "<") {}
            .asInstanceOf[AnyBooleanPhaseParser]
      )
      | expr ~ "<=" ~ ws ~ expr ~> (
        (e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
          new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 <= d2, "<=") {}
            .asInstanceOf[AnyBooleanPhaseParser]
      )
      | expr ~ ">" ~ ws ~ expr ~> (
        (e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
          new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 > d2, ">") {}
            .asInstanceOf[AnyBooleanPhaseParser]
      )
      | expr ~ ">=" ~ ws ~ expr ~> (
        (e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
          new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 >= d2, ">") {}
            .asInstanceOf[AnyBooleanPhaseParser]
      )
      | expr ~ "=" ~ ws ~ expr ~> (
        (e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
          new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 == d2, "==") {}
            .asInstanceOf[AnyBooleanPhaseParser]
      )
      |
      expr ~ ("!=" | "<>") ~ ws ~ expr ~> (
        (e1: AnyNumericPhaseParser, e2: AnyNumericPhaseParser) =>
          new ComparingParser[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 != d2, "!=") {}
            .asInstanceOf[AnyBooleanPhaseParser]
      )
    )
  }
  // format: on

  def expr: Rule1[AnyNumericPhaseParser] = rule {
    term ~ zeroOrMore(
      '+' ~ ws ~ term ~> (
        (
          e: AnyNumericPhaseParser,
          f: AnyNumericPhaseParser
        ) => new BinaryNumericParser[Event, Any, Any, Double](e, f, _ + _, "+").asInstanceOf[AnyNumericPhaseParser]
      )
      | '-' ~ ws ~ term ~> (
        (
          e: AnyNumericPhaseParser,
          f: AnyNumericPhaseParser
        ) => new BinaryNumericParser[Event, Any, Any, Double](e, f, _ - _, "-").asInstanceOf[AnyNumericPhaseParser]
      )
    )
  }

  def term: Rule1[AnyNumericPhaseParser] = rule {
    factor ~
    zeroOrMore(
      '*' ~ ws ~ factor ~> (
        (
          e: AnyNumericPhaseParser,
          f: AnyNumericPhaseParser
        ) => new BinaryNumericParser[Event, Any, Any, Double](e, f, _ * _, "*").asInstanceOf[AnyNumericPhaseParser]
      )
      | '/' ~ ws ~ factor ~> (
        (
          e: AnyNumericPhaseParser,
          f: AnyNumericPhaseParser
        ) => new BinaryNumericParser[Event, Any, Any, Double](e, f, _ / _, "/").asInstanceOf[AnyNumericPhaseParser]
      )
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
      | boolean ~> ((e: OneRowPattern[Event, Boolean]) => (_: Double) => e.extract(nullEvent))
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
      | underscoreExpr ~ "=" ~ ws ~ underscoreExpr ~>
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
      | '/' ~ ws ~ underscoreFactor ~> ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) / f(x))
    )
  }

  def underscoreFactor: Rule1[Double => Double] = rule {
    (
      real ~ ws ~> ((r: OneRowPattern[Event, Double]) => (_: Double) => r.extract(nullEvent))
      | integer ~ ws ~>
      ((r: OneRowPattern[Event, Long]) => (_: Double) => r.extract(nullEvent).toDouble)
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
    (
      (
        d1: OneRowPattern[Event, Double],
        d2: OneRowPattern[Event, Double],
        u: Int
      ) =>
        TimeInterval(
          Window((d1.extract(nullEvent) * u).toLong),
          Window((d2.extract(nullEvent) * u).toLong)
        )
    ))
  }

  def repetitionRange: Rule1[NumericInterval[Long]] = rule {
    ("<" ~ ws ~ repetition ~> ((t: Long) => NumericInterval(0L, Some(t)))
    | "<=" ~ ws ~ repetition ~> ((t: Long) => NumericInterval(0L, Some(t+1L)))
    | ">" ~ ws ~ repetition ~> ((t: Long) => NumericInterval.more(t + 1L))
    | ">=" ~ ws ~ repetition ~> ((t: Long) => NumericInterval.more(t))
    | integer ~ ignoreCase("to") ~ ws ~ repetition ~>
    ((t1: OneRowPattern[Event, Long], t2: Long) => NumericInterval(t1.extract(nullEvent), Some(t2))))
  }

  def repetition: Rule1[Long] = rule {
    integer ~ ignoreCase("times") ~> ((e: OneRowPattern[Event, Long]) => e.extract(nullEvent))
  }

  def time: Rule1[Window] = rule {
    singleTime.+(ws) ~> (
      (ts: Seq[Window]) =>
        Window(ts.foldLeft(0L) { (acc, t) =>
          acc + t.toMillis
        })
    )
  }

  def singleTime: Rule1[Window] = rule {
    real ~ timeUnit ~ ws ~>
    ((i: OneRowPattern[Event, Double], u: Int) => Window((i.extract(nullEvent) * u).toLong))
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

  def real: Rule1[OneRowPattern[Event, Double]] = rule {
    // sign of a number: positive (or empty) = 1, negative = -1
    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1)) ~
    capture(oneOrMore(CharPredicate.Digit) ~ optional('.' ~ oneOrMore(CharPredicate.Digit))) ~ ws
    ~> ((s: Int, i: String) => ConstantPhases[Event, Double](s * i.toDouble)))
  }

  def integer: Rule1[OneRowPattern[Event, Long]] = rule {
    // sign of a number: positive (or empty) = 1, negative = -1
    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1))
    ~ capture(oneOrMore(CharPredicate.Digit)) ~ ws
    ~> ((s: Int, i: String) => ConstantPhases[Event, Long](s * i.toLong)))
  }

  def functionCall: Rule1[AnyNumericPhaseParser] = rule {
    (
      identifier ~ ws ~ "(" ~ ws ~ expr.*(ws ~ "," ~ ws) ~ optional(";" ~ ws ~ underscoreConstraint) ~ ws ~ ")" ~ ws ~>
      ((id: SymbolParser[Event], arguments: Seq[AnyNumericPhaseParser], constraint: Option[Double => Boolean]) => {
        val function = id.symbol.toString.tail.toLowerCase
        val ifCondition: Double => Boolean = constraint.getOrElse(_ => true)
        function match {
          case "lag" => Pattern.Functions.lag(arguments.head).asInstanceOf[AnyNumericPhaseParser]
          case "abs" => Pattern.Functions.abs(arguments.head).asInstanceOf[AnyNumericPhaseParser]
          case "minof" =>
            Reduce[Event, Any](TestFunctions.min(_, _, ifCondition))(
              OneRowPattern[Event, Double](_ => Double.MaxValue).asInstanceOf[AnyNumericPhaseParser],
              arguments: _*
            ).asInstanceOf[AnyNumericPhaseParser]
          case "maxof" =>
            Reduce[Event, Any](TestFunctions.max(_, _, ifCondition))(
              OneRowPattern[Event, Double](_ => Double.MinValue).asInstanceOf[AnyNumericPhaseParser],
              arguments: _*
            ).asInstanceOf[AnyNumericPhaseParser]
          case "avgof" =>
            (Reduce[Event, Any](TestFunctions.plus(_, _, ifCondition))(
              OneRowPattern[Event, Double](_ => 0.0).asInstanceOf[AnyNumericPhaseParser],
              arguments: _*
            ) div
            Reduce[Event, Any](TestFunctions.countNotNan(_, _, ifCondition))(
              OneRowPattern[Event, Double](_ => 0.0).asInstanceOf[AnyNumericPhaseParser],
              arguments: _*
            )).asInstanceOf[AnyNumericPhaseParser]
          case "countof" =>
            Reduce[Event, Any](TestFunctions.countNotNan(_, _, ifCondition))(
              OneRowPattern[Event, Double](_ => Double.MinValue).asInstanceOf[AnyNumericPhaseParser],
              arguments: _*
            ).asInstanceOf[AnyNumericPhaseParser]
          case _ => throw new RuntimeException(s"Unknown function `$function`")
        }
      })
      | identifier ~ ws ~ "(" ~ ws ~ expr ~ ws ~ "," ~ ws ~ time ~ ws ~ ")" ~ ws ~>
      (
        (
          id: SymbolParser[Event],
          arg: AnyNumericPhaseParser,
          win: Window
        ) => {
          val function = id.symbol.toString.tail
          function match {
            case "avg" => Pattern.Functions.avg(arg, win).asInstanceOf[AnyNumericPhaseParser]
            case "sum" => Pattern.Functions.sum(arg, win).asInstanceOf[AnyNumericPhaseParser]
          }
        }
      )
    )
  }

  def identifier: Rule1[SymbolParser[Event]] = rule {
    (capture(CharPredicate.Alpha ~ zeroOrMore(CharPredicate.AlphaNum | '_')) ~ ws ~>
    ((id: String) => SymbolParser[Event](Symbol(id)))
    | '"' ~ capture(oneOrMore(noneOf("\"") | "\"\"")) ~ '"' ~ ws ~>
    ((id: String) => SymbolParser[Event](Symbol(id.replace("\"\"", "\"")))))
  }

  def string: Rule1[OneRowPattern[Event, String]] = rule {
    "'" ~ capture(oneOrMore(noneOf("'") | "''")) ~ "'" ~ ws ~>
    ((str: String) => ConstantPhases[Event, String](str.replace("''", "'")))
  }

  def boolean: Rule1[OneRowPattern[Event, Boolean]] = rule {
    (ignoreCase("true") ~ ws ~> (() => ConstantPhases[Event, Boolean](true))
    | ignoreCase("false") ~ ws ~> (() => ConstantPhases[Event, Boolean](false)) ~ ws)
  }

  def ws = rule {
    quiet(zeroOrMore(anyOf(" \t \n \r")))
  }
}

object TestFunctions {

  def min(d1: Double, d2: Double, cond: Double => Boolean): Double =
    Math.min(if (d1.isNaN) Double.MaxValue else d1, if (d2.isNaN || !cond(d2)) Double.MaxValue else d2)

  def max(d1: Double, d2: Double, cond: Double => Boolean): Double =
    Math.max(if (d1.isNaN) Double.MinValue else d1, if (d2.isNaN || !cond(d2)) Double.MinValue else d2)

  def plus(d1: Double, d2: Double, cond: Double => Boolean): Double = (if (d1.isNaN) 0 else d1) +
  (if (d2.isNaN || !cond(d2)) 0 else d2)

  def countNotNan(d1: Double, d2: Double, cond: Double => Boolean): Double = (if (d1.isNaN) 0 else d1) +
  (if (d2.isNaN || !cond(d2)) 0 else 1)
}
