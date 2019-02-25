//package ru.itclover.tsp.dsl
//
//import org.parboiled2._
//import ru.itclover.tsp.utils.CollectionsOps.StringOps
//import ru.itclover.tsp.aggregators.AggregatorPhases.{PreviousValue, Skip, SegmentsPattern}
//import ru.itclover.tsp.aggregators.accums.{AccumPhase, PushDownAccumInterval}
//import ru.itclover.tsp.core.Time.MaxWindow
//import ru.itclover.tsp.core.{Pattern, Time, Window}
//import ru.itclover.tsp.core.Intervals.{Interval, NumericInterval, TimeInterval}
//import ru.itclover.tsp.io.{Decoder, Extractor, TimeExtractor}
//import ru.itclover.tsp.patterns.Booleans.{BooleanPhaseParser, NotParser}
//import ru.itclover.tsp.patterns.Constants.{ConstPattern, ExtractingPattern, FailurePattern}
//import ru.itclover.tsp.utils.UtilityTypes.ParseException
//import ru.itclover.tsp.patterns.Booleans.{Assert, ComparingPattern}
//import ru.itclover.tsp.patterns.Constants
//import ru.itclover.tsp.patterns.Numerics._
//import ru.itclover.tsp.Segment
//
//object SyntaxParser {
//  // Used for testing purposes
//  def testFieldsSymbolMap(anySymbol: Symbol) = anySymbol
//  def testFieldsIdxMap(anySymbol: Symbol) = 0
//  def testFieldsIdxMap(anyStr: String) = 0
//}
//
//class SyntaxParser[Event, EKey, EItem](val input: ParserInput, idToEKey: Symbol => EKey, toleranceFraction: Double)(
//  implicit timeExtractor: TimeExtractor[Event],
//  extractor: Extractor[Event, EKey, EItem],
//  decodeDouble: Decoder[EItem, Double]
//) extends Parser {
//
//  def const[T](value: T) = ConstPattern[Event, T](value)
//  type AnyPattern = Pattern[Event, Any, Any]
//  type AnyBooleanPattern = BooleanPhaseParser[Event, Any]
//  type AnyNumericPattern = NumericPhaseParser[Event, Any]
//
//  val nullEvent: Event = null.asInstanceOf[Event]
//
//  def start: Rule1[Pattern[Event, Any, Any]] = rule {
//    trileanExpr ~ EOI
//  }
//
//  def trileanExpr: Rule1[AnyPattern] = rule {
//    trileanTerm ~ zeroOrMore(
//      ignoreCase("andthen") ~ ws ~ trileanTerm ~>
//      ((e: AnyPattern, f: AnyPattern) => (e andThen Skip(1, f)).asInstanceOf[AnyPattern])
//      | ignoreCase("and") ~ ws ~ trileanTerm ~>
//      ((e: AnyPattern, f: AnyPattern) => (e togetherWith f).asInstanceOf[AnyPattern])
//      | ignoreCase("or") ~ ws ~ trileanTerm ~>
//      ((e: AnyPattern, f: AnyPattern) => (e either f).asInstanceOf[AnyPattern])
//    )
//  }
//
//  def trileanTerm: Rule1[AnyPattern] = rule {
//    // Exactly is default and ignored for now
//    (nonFatalTrileanFactor ~ ignoreCase("for") ~ ws ~ optional(ignoreCase("exactly") ~ ws ~> (() => true)) ~
//    time ~ range ~ ws ~> (buildRangedForExpr(_, _, _, _))
//    | nonFatalTrileanFactor ~ ignoreCase("for") ~ ws ~
//      (timeWithTolerance | timeBoundedRange) ~ ws ~> (buildForExpr(_, _))
//    | trileanFactor ~ ignoreCase("until") ~ ws ~ booleanExpr ~ optional(range) ~ ws ~>
//    ((c: AnyPattern, b: AnyBooleanPattern, r: Option[Any]) => {
//      (c.timed(MaxWindow).asInstanceOf[AnyBooleanPattern] and
//      Assert(NotParser(b)).asInstanceOf[AnyBooleanPattern]).asInstanceOf[AnyPattern]
//    })
//    | trileanFactor)
//  }
//
//  protected def buildForExpr(phase: AnyPattern, ti: TimeInterval): AnyPattern = {
//    Assert(phase.asInstanceOf[AnyBooleanPattern])
//      .timed(ti)
//      .asInstanceOf[AnyPattern]
//  }
//
//  protected def buildRangedForExpr(
//    phase: AnyPattern,
//    exactly: Option[Boolean],
//    w: Window,
//    range: Interval[Long]
//  ): AnyPattern = {
//    val accum = range match {
//      case _: NumericInterval[Long] =>
//        Pattern.Functions.truthCount(phase.asInstanceOf[AnyBooleanPattern], w)
//      case _: TimeInterval =>
//        Pattern.Functions
//          .truthMillisCount(phase.asInstanceOf[AnyBooleanPattern], w)
//          .asInstanceOf[AccumPhase[Event, Any, Boolean, Long]] // TODO Covariant out
//      case _ => throw ParseException(s"Unknown range type in `for` expr: `$range`")
//    }
//
//    (exactly match {
//      case None => PushDownAccumInterval[Event, Any, Boolean, Long](accum, range)
//      case Some(_) => accum
//    }).flatMap(count => {
//        if (range.contains(count)) {
//          ConstPattern(count)
//        } else {
//          FailurePattern(s"Window ($range) not fully accumulated ($count)")
//        }
//      })
//      .asInstanceOf[AnyPattern]
//  }
//
//  // format: off
//
//  def nonFatalTrileanFactor: Rule1[AnyPattern] = rule {
//    booleanExpr ~> { b: AnyBooleanPattern => b } | '(' ~ trileanExpr ~ ')' ~ ws
//  }
//
//  def trileanFactor: Rule1[AnyPattern] = rule {
//    booleanExpr ~> { b: AnyBooleanPattern => Assert(b) } | '(' ~ trileanExpr ~ ')' ~ ws
//  }
//
//  def booleanExpr: Rule1[AnyBooleanPattern] = rule {
//    booleanTerm ~ zeroOrMore(
//      ignoreCase("or") ~ ws ~ booleanTerm ~>
//      ((e: AnyBooleanPattern, f: AnyBooleanPattern) => ComparingPattern(e, f)((a, b) => a | b, "or").asInstanceOf[AnyBooleanPattern])
//      | ignoreCase("xor") ~ ws ~ booleanTerm ~>
//        ((e: AnyBooleanPattern, f: AnyBooleanPattern) => ComparingPattern(e, f)((a, b) => a ^ b, "xor").asInstanceOf[AnyBooleanPattern])
//    )
//  }
//
//  def booleanTerm: Rule1[AnyBooleanPattern] = rule {
//    booleanFactor ~ zeroOrMore(
//      ignoreCase("and") ~ !ignoreCase("then") ~ ws ~ booleanFactor ~>
//      ((e: AnyBooleanPattern, f: AnyBooleanPattern) => ComparingPattern(e, f)((a, b) => a & b, "and").asInstanceOf[AnyBooleanPattern])
//    )
//  }
//
//  def booleanFactor: Rule1[AnyBooleanPattern] = rule {
//    comparison |
//      boolean ~> ((b: ConstPattern[Event, Boolean]) => b.asInstanceOf[AnyBooleanPattern]) |
//      "(" ~ booleanExpr ~ ")" ~ ws | "not" ~ booleanExpr ~> ((b: AnyBooleanPattern) => NotParser(b))
//  }
//
//  def comparison: Rule1[AnyBooleanPattern] = rule {
//    (
//      expr ~ "<" ~ ws ~ expr ~> (
//        (e1: AnyNumericPattern, e2: AnyNumericPattern) =>
//          ComparingPattern[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 < d2, "<")
//            .asInstanceOf[AnyBooleanPattern]
//      )
//      | expr ~ "<=" ~ ws ~ expr ~> (
//        (e1: AnyNumericPattern, e2: AnyNumericPattern) =>
//          ComparingPattern[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 <= d2, "<=")
//            .asInstanceOf[AnyBooleanPattern]
//      )
//      | expr ~ ">" ~ ws ~ expr ~> (
//        (e1: AnyNumericPattern, e2: AnyNumericPattern) =>
//          ComparingPattern[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 > d2, ">")
//            .asInstanceOf[AnyBooleanPattern]
//      )
//      | expr ~ ">=" ~ ws ~ expr ~> (
//        (e1: AnyNumericPattern, e2: AnyNumericPattern) =>
//          ComparingPattern[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 >= d2, ">")
//            .asInstanceOf[AnyBooleanPattern]
//      )
//      | expr ~ "=" ~ ws ~ expr ~> (
//        (e1: AnyNumericPattern, e2: AnyNumericPattern) =>
//          ComparingPattern[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 == d2, "==")
//            .asInstanceOf[AnyBooleanPattern]
//      )
//      |
//      expr ~ ("!=" | "<>") ~ ws ~ expr ~> (
//        (e1: AnyNumericPattern, e2: AnyNumericPattern) =>
//          ComparingPattern[Event, Any, Any, Double](e1, e2)((d1, d2) => d1 != d2, "!=")
//            .asInstanceOf[AnyBooleanPattern]
//      )
//    )
//  }
//  // format: on
//
//  def expr: Rule1[AnyNumericPattern] = rule {
//    term ~ zeroOrMore(
//      '+' ~ ws ~ term ~> (
//        (
//          e: AnyNumericPattern,
//          f: AnyNumericPattern
//        ) => new BinaryNumericParser[Event, Any, Any, Double](e, f, _ + _, "+").asInstanceOf[AnyNumericPattern]
//      )
//      | '-' ~ ws ~ term ~> (
//        (
//          e: AnyNumericPattern,
//          f: AnyNumericPattern
//        ) => new BinaryNumericParser[Event, Any, Any, Double](e, f, _ - _, "-").asInstanceOf[AnyNumericPattern]
//      )
//    )
//  }
//
//  def term: Rule1[AnyNumericPattern] = rule {
//    factor ~
//    zeroOrMore(
//      '*' ~ ws ~ factor ~> (
//        (
//          e: AnyNumericPattern,
//          f: AnyNumericPattern
//        ) => BinaryNumericParser[Event, Any, Any, Double](e, f, _ * _, "*").asInstanceOf[AnyNumericPattern]
//      )
//      | '/' ~ ws ~ factor ~> (
//        (
//          e: AnyNumericPattern,
//          f: AnyNumericPattern
//        ) => BinaryNumericParser[Event, Any, Any, Double](e, f, _ / _, "/").asInstanceOf[AnyNumericPattern]
//      )
//    )
//  }
//
//  def factor: Rule1[AnyNumericPattern] = rule {
//    (
//      real ~> (_.asInstanceOf[AnyNumericPattern])
//      | long ~> (_.asInstanceOf[AnyNumericPattern])
//      | functionCall
//      | fieldValue ~> (_.asInstanceOf[AnyNumericPattern])
//      | '(' ~ expr ~ ')' ~ ws
//    )
//  }
//
//  def underscoreConstraint: Rule1[Double => Boolean] = rule {
//    underscoreConjunction ~ zeroOrMore(
//      ignoreCase("or") ~ ws ~ underscoreConjunction ~>
//      ((e: Double => Boolean, f: Double => Boolean) => (x: Double) => e(x) || f(x))
//      | ignoreCase("xor") ~ ws ~ underscoreConjunction ~>
//      ((e: Double => Boolean, f: Double => Boolean) => (x: Double) => e(x) != f(x))
//    )
//  }
//
//  def underscoreConjunction: Rule1[Double => Boolean] = rule {
//    underscoreCond ~ zeroOrMore(
//      ignoreCase("and") ~ ws ~ underscoreCond ~>
//      ((e: Double => Boolean, f: Double => Boolean) => (x: Double) => e(x) && f(x))
//    )
//  }
//
//  def underscoreCond: Rule1[Double => Boolean] = rule {
//    (
//      underscoreComparison
//      | boolean ~> ((e: ConstPattern[Event, Boolean]) => (_: Double) => e.value)
//      | '(' ~ underscoreConstraint ~ ')'
//      | ignoreCase("not") ~ underscoreCond ~> ((e: Double => Boolean) => (x: Double) => !e(x))
//    )
//  }
//
//  def underscoreComparison: Rule1[Double => Boolean] = rule {
//    (
//      underscoreExpr ~ "<" ~ ws ~ underscoreExpr ~>
//      ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) < f(x))
//      | underscoreExpr ~ "<=" ~ ws ~ underscoreExpr ~>
//      ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) <= f(x))
//      | underscoreExpr ~ ">" ~ ws ~ underscoreExpr ~>
//      ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) > f(x))
//      | underscoreExpr ~ ">=" ~ ws ~ underscoreExpr ~>
//      ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) >= f(x))
//      | underscoreExpr ~ "=" ~ ws ~ underscoreExpr ~>
//      ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) == f(x))
//      | underscoreExpr ~ ("!=" | "<>") ~ ws ~ underscoreExpr ~>
//      ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) != f(x))
//    )
//  }
//
//  def underscoreExpr: Rule1[Double => Double] = rule {
//    underscoreTerm ~
//    zeroOrMore(
//      '+' ~ ws ~ underscoreTerm ~> ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) + f(x))
//      | '-' ~ ws ~ underscoreTerm ~> ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) - f(x))
//    )
//  }
//
//  def underscoreTerm: Rule1[Double => Double] = rule {
//    underscoreFactor ~
//    zeroOrMore(
//      '*' ~ ws ~ underscoreFactor ~> ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) * f(x))
//      | '/' ~ ws ~ underscoreFactor ~> ((e: Double => Double, f: Double => Double) => (x: Double) => e(x) / f(x))
//    )
//  }
//
//  def underscoreFactor: Rule1[Double => Double] = rule {
//    (
//      real ~ ws ~> ((r: ConstPattern[Event, Double]) => (_: Double) => r.value)
//      | long ~ ws ~>
//      ((r: ConstPattern[Event, Long]) => (_: Double) => r.value.toDouble)
//      | str("_") ~ ws ~> (() => (x: Double) => x)
//      | '(' ~ underscoreExpr ~ ')' ~ ws
//    )
//  }
//
//  def range: Rule1[Interval[Long]] = rule {
//    timeRange | repetitionRange
//  }
//
//  def timeRange: Rule1[TimeInterval] = rule {
//    ("<" ~ ws ~ time ~> ((t: Window) => Time.less(t))
//    | "<=" ~ ws ~ time ~> ((t: Window) => Time.less(t))
//    | ">" ~ ws ~ time ~> ((t: Window) => Time.more(t))
//    | ">=" ~ ws ~ time ~> ((t: Window) => Time.more(t))
//    | timeBoundedRange)
//  }
//
//  def timeBoundedRange: Rule1[TimeInterval] = rule {
//    (time ~ ignoreCase("to") ~ ws ~ time ~>
//    ((t1: Window, t2: Window) => TimeInterval(t1, t2))
//    | real ~ ignoreCase("to") ~ ws ~ real ~ timeUnit ~>
//    (
//      (
//        d1: ConstPattern[Event, Double],
//        d2: ConstPattern[Event, Double],
//        u: Int
//      ) =>
//        TimeInterval(
//          Window((d1.value * u).toLong),
//          Window((d2.value * u).toLong)
//        )
//    ))
//  }
//
//  def repetitionRange: Rule1[NumericInterval[Long]] = rule {
//    ("<" ~ ws ~ repetition ~> ((t: Long) => NumericInterval(0L, Some(t)))
//    | "<=" ~ ws ~ repetition ~> ((t: Long) => NumericInterval(0L, Some(t + 1L)))
//    | ">" ~ ws ~ repetition ~> ((t: Long) => NumericInterval.more(t + 1L))
//    | ">=" ~ ws ~ repetition ~> ((t: Long) => NumericInterval.more(t))
//    | long ~ ignoreCase("to") ~ ws ~ repetition ~>
//    ((t1: ConstPattern[Event, Long], t2: Long) => NumericInterval(t1.value, Some(t2))))
//  }
//
//  def repetition: Rule1[Long] = rule {
//    long ~ ignoreCase("times") ~> ((e: ConstPattern[Event, Long]) => e.value)
//  }
//
//  def time: Rule1[Window] = rule {
//    singleTime.+(ws) ~> (
//      (ts: Seq[Window]) =>
//        Window(ts.foldLeft(0L) { (acc, t) =>
//          acc + t.toMillis
//        })
//    )
//  }
//
//  def timeWithTolerance: Rule1[TimeInterval] = rule {
//    (time ~ ws ~ "+-" ~ ws ~ time ~> (
//      (
//        win: Window,
//        tol: Window
//      ) => TimeInterval(Window(Math.max(win.toMillis - tol.toMillis, 0)), Window(win.toMillis + tol.toMillis))
//    )
//    | time ~ ws ~ "+-" ~ ws ~ real ~ ws ~ "%" ~> (
//      (
//        win: Window,
//        tolPc: ConstPattern[Event, Double]
//      ) => {
//        val tol = (tolPc.value * 0.01 * win.toMillis).toLong
//        TimeInterval(Window(Math.max(win.toMillis - tol, 0)), Window(win.toMillis + tol))
//      }
//    )
//    | time ~> ((win: Window) => {
//      val tol = (win.toMillis * toleranceFraction).toLong
//      TimeInterval(Window(Math.max(win.toMillis - tol, 0)), Window(win.toMillis + tol))
//    }))
//  }
//
//  def singleTime: Rule1[Window] = rule {
//    real ~ timeUnit ~ ws ~>
//    ((i: ConstPattern[Event, Double], u: Int) => Window((i.value * u).toLong))
//  }
//
//  def timeUnit: Rule1[Int] = rule {
//    (ignoreCase("seconds") ~> (() => 1000)
//    | ignoreCase("sec") ~> (() => 1000)
//    | ignoreCase("minutes") ~> (() => 60000)
//    | ignoreCase("min") ~> (() => 60000)
//    | ignoreCase("milliseconds") ~> (() => 1)
//    | ignoreCase("ms") ~> (() => 1)
//    | ignoreCase("hours") ~> (() => 3600000)
//    | ignoreCase("hr") ~> (() => 3600000))
//  }
//
//  def real: Rule1[ConstPattern[Event, Double]] = rule {
//    // sign of a number: positive (or empty) = 1, negative = -1
//    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1)) ~
//    capture(oneOrMore(CharPredicate.Digit) ~ optional('.' ~ oneOrMore(CharPredicate.Digit))) ~ ws
//    ~> ((sign: Int, i: String) => const[Double](sign * i.toDouble)))
//  }
//
//  def long: Rule1[ConstPattern[Event, Long]] = rule {
//    // sign of a number: positive (or empty) = 1, negative = -1
//    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1))
//    ~ capture(oneOrMore(CharPredicate.Digit)) ~ ws
//    ~> ((s: Int, i: String) => const[Long](s * i.toLong)))
//  }
//
//  def functionCall: Rule1[AnyNumericPattern] = rule {
//    (
//      anyWord ~ ws ~ "(" ~ ws ~ expr.*(ws ~ "," ~ ws) ~ optional(";" ~ ws ~ underscoreConstraint) ~ ws ~ ")" ~ ws ~>
//      ((function: String, arguments: Seq[AnyNumericPattern], constraint: Option[Double => Boolean]) => {
//        val ifCondition: Double => Boolean = constraint.getOrElse(_ => true)
//        function.toLowerCase match {
//          case "lag" => Pattern.Functions.lag(arguments.head).asInstanceOf[AnyNumericPattern]
//          case "abs" => Pattern.Functions.call1(Math.abs, "abs", arguments.head).asInstanceOf[AnyNumericPattern]
//          case "sin" => Pattern.Functions.call1(Math.sin, "sin", arguments.head).asInstanceOf[AnyNumericPattern]
//          case "cos" => Pattern.Functions.call1(Math.cos, "cos", arguments.head).asInstanceOf[AnyNumericPattern]
//          case "tan" | "tg" =>
//            Pattern.Functions.call1(Math.tan, "tan", arguments.head).asInstanceOf[AnyNumericPattern]
//          case "cot" | "ctg" =>
//            Pattern.Functions.call1(1.0 / Math.tan(_), "cot", arguments.head).asInstanceOf[AnyNumericPattern]
//          case "sind" =>
//            Pattern.Functions
//              .call1((x: Double) => Math.sin(x * Math.PI / 180.0), "sind", arguments.head)
//              .asInstanceOf[AnyNumericPattern]
//          case "cosd" =>
//            Pattern.Functions
//              .call1((x: Double) => Math.cos(x * Math.PI / 180.0), "cosd", arguments.head)
//              .asInstanceOf[AnyNumericPattern]
//          case "tand" | "tgd" =>
//            Pattern.Functions
//              .call1((x: Double) => Math.tan(x * Math.PI / 180.0), "tand", arguments.head)
//              .asInstanceOf[AnyNumericPattern]
//          case "cotd" | "ctgd" =>
//            Pattern.Functions
//              .call1((x: Double) => 1.0 / Math.tan(x * Math.PI / 180.0), "cotd", arguments.head)
//              .asInstanceOf[AnyNumericPattern]
//          case "exp" =>
//            Pattern.Functions.call1(Math.exp, "exp", arguments.head).asInstanceOf[AnyNumericPattern]
//          case "ln" =>
//            Pattern.Functions.call1(Math.log, "ln", arguments.head).asInstanceOf[AnyNumericPattern]
//          case "log" =>
//            Pattern.Functions
//              .call2((x, y) => Math.log(y) / Math.log(x), "log", arguments.head, arguments(1))
//              .asInstanceOf[AnyNumericPattern]
//          case "sigmoid" =>
//            Pattern.Functions
//              .call2((x, y) => 1.0 / (1 + Math.exp(-2 * x * y)), "sigmoid", arguments.head, arguments(1))
//              .asInstanceOf[AnyNumericPattern]
//          case "minof" =>
//            Reduce[Event, Any](TestFunctions.min(_, _, ifCondition))(
//              const[Double](Double.MaxValue).asInstanceOf[AnyNumericPattern],
//              arguments: _*
//            ).asInstanceOf[AnyNumericPattern]
//          case "maxof" =>
//            Reduce[Event, Any](TestFunctions.max(_, _, ifCondition))(
//              const[Double](Double.MinValue).asInstanceOf[AnyNumericPattern],
//              arguments: _*
//            ).asInstanceOf[AnyNumericPattern]
//          case "avgof" =>
//            (Reduce[Event, Any](TestFunctions.plus(_, _, ifCondition))(
//              const[Double](0.0).asInstanceOf[AnyNumericPattern],
//              arguments: _*
//            ) div
//            Reduce[Event, Any](TestFunctions.countNotNan(_, _, ifCondition))(
//              const[Double](0.0).asInstanceOf[AnyNumericPattern],
//              arguments: _*
//            )).asInstanceOf[AnyNumericPattern]
//          case "countof" =>
//            Reduce[Event, Any](TestFunctions.countNotNan(_, _, ifCondition))(
//              const[Double](0.0).asInstanceOf[AnyNumericPattern],
//              arguments: _*
//            ).asInstanceOf[AnyNumericPattern]
//          case _ => throw new RuntimeException(s"Unknown function `$function`")
//        }
//      })
//      | anyWord ~ ws ~ "(" ~ ws ~ expr ~ ws ~ "," ~ ws ~ time ~ ws ~ ")" ~ ws ~>
//      (
//        (
//          function: String,
//          arg: AnyNumericPattern,
//          win: Window
//        ) => {
//          function match {
//            case "avg" => Pattern.Functions.avg(arg, win).asInstanceOf[AnyNumericPattern]
//            case "sum" => Pattern.Functions.sum(arg, win).asInstanceOf[AnyNumericPattern]
//            case "lag" => Pattern.Functions.lag(arg, win).asInstanceOf[AnyNumericPattern]
//          }
//        }
//      )
//    )
//  }
//
//  def anyWord: Rule1[String] = rule {
//    capture(CharPredicate.Alpha ~ zeroOrMore(CharPredicate.AlphaNum | '_')) ~ ws
//  }
//
//  def anyWordInDblQuotes: Rule1[String] = rule {
//    '"' ~ capture(oneOrMore(noneOf("\"") | "\"\"")) ~ '"' ~ ws
//  }
//
//  def fieldValue: Rule1[ExtractingPattern[Event, EKey, EItem, Double]] = rule {
//    (anyWord ~> ((id: String) => ExtractingPattern(idToEKey(id.toSymbol), id.toSymbol))
//    | anyWordInDblQuotes ~>
//    ((id: String) => {
//      val clean = id.replace("\"\"", "\"").toSymbol
//      ExtractingPattern(idToEKey(clean), clean)
//    }))
//  }
//
//  def boolean: Rule1[ConstPattern[Event, Boolean]] = rule {
//    (ignoreCase("true") ~ ws ~> (() => const[Boolean](true))
//    | ignoreCase("false") ~ ws ~> (() => const[Boolean](false)) ~ ws)
//  }
//
//  def ws = rule {
//    quiet(zeroOrMore(anyOf(" \t \n \r")))
//  }
//}
//
//object TestFunctions {
//
//  def min(d1: Double, d2: Double, cond: Double => Boolean): Double =
//    Math.min(if (d1.isNaN) Double.MaxValue else d1, if (d2.isNaN || !cond(d2)) Double.MaxValue else d2)
//
//  def max(d1: Double, d2: Double, cond: Double => Boolean): Double =
//    Math.max(if (d1.isNaN) Double.MinValue else d1, if (d2.isNaN || !cond(d2)) Double.MinValue else d2)
//
//  def plus(d1: Double, d2: Double, cond: Double => Boolean): Double = (if (d1.isNaN) 0 else d1) +
//  (if (d2.isNaN || !cond(d2)) 0 else d2)
//
//  def countNotNan(d1: Double, d2: Double, cond: Double => Boolean): Double = (if (d1.isNaN) 0 else d1) +
//  (if (d2.isNaN || !cond(d2)) 0 else 1)
//}
