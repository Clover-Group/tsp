package ru.itclover.tsp.dsl

import org.parboiled2._
import ru.itclover.tsp.core.Intervals.{Interval, NumericInterval, TimeInterval}
import ru.itclover.tsp.core.Time.{MaxWindow, MinWindow}
import ru.itclover.tsp.core.{Time, Window, _}
import UtilityTypes.ParseException

import scala.language.higherKinds
import scala.reflect.ClassTag

// TODO@trolley813: Adapt to the new `v2` single-state patterns
//object ASTBuilder {
//  // Used for testing purposes
//  def testFieldsSymbolMap(anySymbol: Symbol) = anySymbol
//  def testFieldsIdxMap(anySymbol: Symbol) = 0
//  def testFieldsIdxMap(anyStr: String) = 0
//}

class ASTBuilder(val input: ParserInput, toleranceFraction: Double, fieldsTags: Map[Symbol, ClassTag[_]])
    extends Parser {

  // TODO: Move to params
  @transient implicit val funReg: FunctionRegistry = DefaultFunctionRegistry

  def start: Rule1[AST] = rule {
    trileanExpr ~ EOI
  }

  def trileanExpr: Rule1[AST] = rule {
    trileanTerm ~ zeroOrMore(
      ignoreCase("andthen") ~ ws ~ trileanTerm ~>
      ((e: AST, f: AST) => AndThen(e, f))
      | ignoreCase("and") ~ ws ~ trileanTerm ~>
      ((e: AST, f: AST) => FunctionCall('and, Seq(e, f)))
      | ignoreCase("or") ~ ws ~ trileanTerm ~>
      ((e: AST, f: AST) => FunctionCall('or, Seq(e, f)))
    )
  }

  // The following comment disables IDEA's type-aware inspection for a region (until the line with the same comment)
  /*_*/
  def trileanTerm: Rule1[AST] = rule {
    // Exactly is default and ignored for now
    (trileanFactor ~ ignoreCase("for") ~ ws ~ optional(ignoreCase("exactly") ~ ws ~> (() => true)) ~
    time ~ range ~ ws ~> (ForWithInterval(_, _, _, _))
    | nonFatalTrileanFactor ~ ignoreCase("for") ~ ws ~
    (timeWithTolerance | timeBoundedRange) ~ ws ~> (buildForExpr(_, _))
    | trileanFactor ~ ignoreCase("until") ~ ws ~ booleanExpr ~ optional(range) ~ ws ~>
    ((c: AST, b: AST, r: Option[Any]) => {
      val until = Assert(FunctionCall('not, Seq(b)))
      //val window = Window(86400000) // 24 hours
      val timedCondition = Timer(c, TimeInterval(MaxWindow, MaxWindow), Some(MinWindow))
      FunctionCall('and, Seq(timedCondition, until))
    })
    | trileanFactor)
  }
  /*_*/

  protected def buildForExpr(phase: AST, ti: TimeInterval): AST = {
    Timer(Assert(phase.asInstanceOf[AST]), ti)
  }

  // format: off

  def nonFatalTrileanFactor: Rule1[AST] = rule {
    booleanExpr | '(' ~ trileanExpr ~ ')' ~ ws
  }

  def trileanFactor: Rule1[AST] = rule {
    booleanExpr ~> { b: AST => Assert(b) } | '(' ~ trileanExpr ~ ')' ~ ws
  }

  def booleanExpr: Rule1[AST] = rule {
    booleanTerm ~ zeroOrMore(
      ignoreCase("or") ~ ws ~ booleanTerm ~>
      ((e: AST, f: AST) => FunctionCall('or, Seq(e, f)))
      | ignoreCase("xor") ~ ws ~ booleanTerm ~>
        ((e: AST, f: AST) => FunctionCall('xor, Seq(e, f)))
    )
  }

  def booleanTerm: Rule1[AST] = rule {
    booleanFactor ~ zeroOrMore(
      ignoreCase("and") ~ !ignoreCase("then") ~ ws ~ booleanFactor ~>
      ((e: AST, f: AST) => FunctionCall('and, Seq(e, f)))
    )
  }

  def booleanFactor: Rule1[AST] = rule {
    comparison |
      boolean |
      "(" ~ booleanExpr ~ ")" ~ ws | "not" ~ booleanExpr ~> ((b: AST) => FunctionCall('not, Seq(b)))
  }

  def comparison: Rule1[AST] = rule {
    (
      expr ~ "<" ~ ws ~ expr ~> (
        (e1: AST, e2: AST) => FunctionCall('lt, Seq(e1, e2))
      )
      | expr ~ "<=" ~ ws ~ expr ~> (
        (e1: AST, e2: AST) => FunctionCall('le, Seq(e1, e2))
      )
      | expr ~ ">" ~ ws ~ expr ~> (
        (e1: AST, e2: AST) => FunctionCall('gt, Seq(e1, e2))
      )
      | expr ~ ">=" ~ ws ~ expr ~> (
        (e1: AST, e2: AST) => FunctionCall('ge, Seq(e1, e2))
      )
      | expr ~ "=" ~ ws ~ expr ~> (
        (e1: AST, e2: AST) => FunctionCall('eq, Seq(e1, e2))
      )
      |
      expr ~ ("!=" | "<>") ~ ws ~ expr ~> (
        (e1: AST, e2: AST) => FunctionCall('ne, Seq(e1, e2))
      )
    )
  }
  // format: on

  def expr: Rule1[AST] = rule {
    term ~ zeroOrMore(
      '+' ~ ws ~ term ~> (
        (
          e: AST,
          f: AST
        ) => FunctionCall('add, Seq(e, f))
      )
      | '-' ~ ws ~ term ~> (
        (
          e: AST,
          f: AST
        ) => FunctionCall('sub, Seq(e, f))
      )
    )
  }

  def term: Rule1[AST] = rule {
    factor ~
    zeroOrMore(
      '*' ~ ws ~ factor ~> (
        (
          e: AST,
          f: AST
        ) => FunctionCall('mul, Seq(e, f))
      )
      | '/' ~ ws ~ factor ~> (
        (
          e: AST,
          f: AST
        ) => FunctionCall('div, Seq(e, f))
      )
    )
  }

  /*_*/
  def factor: Rule1[AST] = rule {
    (
      real
      | boolean
      | string
      | functionCall
      | fieldValue
      | '(' ~ expr ~ ')' ~ ws
    ) ~ optional(ws ~ ignoreCase("as") ~ ws ~ typeName ~ ws) ~>
    (
      (
        f: AST, // expression
        t: Option[ASTType] // which optionally can be cast to this type
      ) =>
        t match {
          case Some(value) => Cast(f, value)
          case None        => f
        }
      )
  }
  /*_*/

  def typeName: Rule1[ASTType] = rule {
    (
      ignoreCase("int32") ~> (() => IntASTType)
      | ignoreCase("int64") ~> (() => LongASTType)
      | ignoreCase("float64") ~> (() => DoubleASTType)
      | ignoreCase("boolean") ~> (() => BooleanASTType)
      | ignoreCase("string") ~> (() => StringASTType)
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
      | boolean ~> ((e: Constant[Boolean]) => (_: Double) => e.value)
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
      real ~ ws ~> ((r: Constant[Double]) => (_: Double) => r.value)
      | long ~ ws ~>
      ((r: Constant[Long]) => (_: Double) => r.value.toDouble)
      | str("_") ~ ws ~> (() => (x: Double) => x)
      | '(' ~ underscoreExpr ~ ')' ~ ws
    )
  }

  def range: Rule1[Interval[Long]] = rule {
    timeRange | repetitionRange
  }

  def timeRange: Rule1[TimeInterval] = rule {
    ("<" ~ ws ~ time ~> ((t: Window) => Time.less(t))
    | "<=" ~ ws ~ time ~> ((t: Window) => Time.less(t))
    | ">" ~ ws ~ time ~> ((t: Window) => Time.more(t))
    | ">=" ~ ws ~ time ~> ((t: Window) => Time.more(t))
    | timeBoundedRange)
  }

  def timeBoundedRange: Rule1[TimeInterval] = rule {
    (time ~ ignoreCase("to") ~ ws ~ time ~>
    ((t1: Window, t2: Window) => TimeInterval(t1, t2))
    | real ~ ignoreCase("to") ~ ws ~ real ~ timeUnit ~>
    (
      (
        d1: Constant[Double],
        d2: Constant[Double],
        u: Int
      ) =>
        TimeInterval(
          Window((d1.value * u).toLong),
          Window((d2.value * u).toLong)
        )
      ))
  }

  def repetitionRange: Rule1[NumericInterval[Long]] = rule {
    ("<" ~ ws ~ repetition ~> ((t: Long) => NumericInterval(0L, Some(t)))
    | "<=" ~ ws ~ repetition ~> ((t: Long) => NumericInterval(0L, Some(t + 1L)))
    | ">" ~ ws ~ repetition ~> ((t: Long) => NumericInterval.more(t + 1L))
    | ">=" ~ ws ~ repetition ~> ((t: Long) => NumericInterval.more(t))
    | long ~ ignoreCase("to") ~ ws ~ repetition ~>
    ((t1: Constant[Long], t2: Long) => NumericInterval(t1.value, Some(t2))))
  }

  def repetition: Rule1[Long] = rule {
    long ~ ignoreCase("times") ~> ((e: Constant[Long]) => e.value)
  }

  def time: Rule1[Window] = rule {
    singleTime.+(ws) ~> (
      (ts: Seq[Window]) =>
        Window(ts.foldLeft(0L) { (acc, t) =>
          acc + t.toMillis
        })
      )
  }

  def timeWithTolerance: Rule1[TimeInterval] = rule {
    (time ~ ws ~ "+-" ~ ws ~ time ~> (
      (
        win: Window,
        tol: Window
      ) => TimeInterval(Window(Math.max(win.toMillis - tol.toMillis, 0)), Window(win.toMillis + tol.toMillis))
    )
    | time ~ ws ~ "+-" ~ ws ~ real ~ ws ~ "%" ~> (
      (
        win: Window,
        tolPc: Constant[Double]
      ) => {
        val tol = (tolPc.value * 0.01 * win.toMillis).toLong
        TimeInterval(Window(Math.max(win.toMillis - tol, 0)), Window(win.toMillis + tol))
      }
    )
    | time ~> ((win: Window) => {
      val tol = (win.toMillis * toleranceFraction).toLong
      TimeInterval(Window(Math.max(win.toMillis - tol, 0)), Window(win.toMillis + tol))
    }))
  }

  def singleTime: Rule1[Window] = rule {
    real ~ timeUnit ~ ws ~>
    ((i: Constant[Double], u: Int) => Window((i.value * u).toLong))
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

  def real: Rule1[Constant[Double]] = rule {
    // sign of a number: positive (or empty) = 1, negative = -1
    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1)) ~
    capture(oneOrMore(CharPredicate.Digit) ~ optional('.' ~ oneOrMore(CharPredicate.Digit))) ~ ws
    ~> ((sign: Int, i: String) => Constant(sign * i.toDouble)))
  }

  def long: Rule1[Constant[Long]] = rule {
    // sign of a number: positive (or empty) = 1, negative = -1
    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1))
    ~ capture(oneOrMore(CharPredicate.Digit)) ~ ws
    ~> ((s: Int, i: String) => Constant(s * i.toLong)))
  }

  /*_*/
  def functionCall: Rule1[AST] = rule {
    (
      anyWord ~ ws ~ "(" ~ ws ~ expr.*(ws ~ "," ~ ws) ~ optional(";" ~ ws ~ underscoreConstraint) ~ ws ~ ")" ~ ws ~>
      ((function: String, arguments: Seq[AST], constraint: Option[Double => Boolean]) => {
        // TODO: Convention about function naming?
        val normalisedFunction = function.toLowerCase
        val c = constraint.getOrElse((_: Double) => true)
        normalisedFunction match {
          case x if x.endsWith("of") =>
            ReducerFunctionCall(
              Symbol(normalisedFunction),
              (x: Result[Any]) => c(x.getOrElse(Double.NaN).asInstanceOf[Double]),
              arguments
            )
          case "lag" =>
            if (arguments.length > 1) throw ParseException("Lag should use only 1 argument when called without window")
            AggregateCall(Lag, arguments.head, Window(1), Some(Window(0)))
          case _ => FunctionCall(Symbol(normalisedFunction), arguments)
        }
      })
      | anyWord ~ ws ~ "(" ~ ws ~ expr ~ ws ~ "," ~ ws ~ time ~ ws ~ ")" ~ ws ~>
      (
        (
          function: String,
          arg: AST,
          win: Window
        ) => {
          AggregateCall(AggregateFn.fromSymbol(Symbol(function)), arg, win)
        }
      )
    )
  }
  /*_*/

  def anyWord: Rule1[String] = rule {
    ((capture(CharPredicate.Alpha ~ zeroOrMore(CharPredicate.AlphaNum | '_')) ~ ws)
    | (anyWordInDblQuotes ~> ((id: String) => id.replace("\"\"", "\""))))
  }

  def anyWordInDblQuotes: Rule1[String] = rule {
    '"' ~ capture(oneOrMore(noneOf("\"") | "\"\"")) ~ '"' ~ ws
  }

  def anyWordInSingleQuotes: Rule1[String] = rule {
    '\'' ~ capture(oneOrMore(noneOf("'") | "''")) ~ '\'' ~ ws
  }

  def fieldValue: Rule1[Identifier] = rule {
    anyWord ~> ((id: String) => {
      fieldsTags.get(Symbol(id)) match {
        case Some(tag) => Identifier(Symbol(id), tag)
        case None      => Identifier(Symbol(id), ClassTag.Double)
        // case None      => throw ParseException(s"Unknown identifier (field) $id")
      }
    })
  }

  def boolean: Rule1[Constant[Boolean]] = rule {
    (ignoreCase("true") ~ ws ~> (() => Constant(true))
    | ignoreCase("false") ~ ws ~> (() => Constant(false)) ~ ws)
  }

  def string: Rule1[Constant[String]] = rule {
    (anyWordInSingleQuotes ~> ((id: String) => Constant(id.replace("''", "'"))))
  }

  def ws = rule {
    quiet(zeroOrMore(anyOf(" \t \n \r")))
  }
}
