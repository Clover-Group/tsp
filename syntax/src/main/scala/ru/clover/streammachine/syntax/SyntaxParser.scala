package ru.clover.streammachine.syntax

import parseback._
import ru.itclover.streammachine.aggregators.{AccumulationPhase, NumericAccumulatedState}
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.core.{PhaseParser, Time, TimeInterval, Window}
import ru.itclover.streammachine.phases.BooleanPhases.{AndParser, GreaterParser, NotParser}
import ru.itclover.streammachine.phases.ConstantPhases.OneRowPhaseParser
import ru.itclover.streammachine.phases.TimePhases.Timed
import ru.itclover.streammachine.phases.{ConstantPhases, NoState}

import scala.concurrent.duration.Duration


class SyntaxParser[Event: TimeExtractor : FieldExtractor]() {

  val parseback: Parser[PhaseParser[Event, _, _]] = {
    import _root_.parseback._

    lazy val double: Parser[Double] = """[-+]?\d+(?:\.\d+)?""".r ^^ ((_, str) => str.toDouble)
    lazy val long: Parser[Long] = """[-+]?\d+""".r ^^ ((_, str) => str.toLong)

    lazy val numberParser: Parser[PhaseParser[Event, NoState, Double]] = double ^^ ((_, str) => ConstantPhases(str))

    //todo fix types
    lazy val field: Parser[PhaseParser[Event, NoState, Double]] = "'" ~ """\w+(\d|\w)+""".r ^^ ((_, _, fieldName) => {
      OneRowPhaseParser(implicitly[FieldExtractor[Event]].getField(fieldName)
        .getOrElse(throw new RuntimeException(s"No such field: $fieldName")).asInstanceOf[Function[Event, Double]])
    })

    lazy val space: Parser[Unit] = """\s+""".r ^^ ((_, _) => ())

    lazy val window: Parser[Window] = long ~ space.*() ~
      """d|day|h|hour|min|minute|s|sec|second|ms|milli|millisecond|µs|micro|microsecond`""".r ^^
      ((_, l, _, timeUnit) => Window(Duration.apply(l, timeUnit).toMillis))

    lazy val functionName: Parser[String] = "sum|avg|count".r //todo

    lazy val windowFunction: Parser[PhaseParser[Event, _, Double]] = functionName ~ "(" ~> numericParser ~ (space.+() ~> "in" <~ space.+()) ~ window <~ ")" ^^ ((_, inner, _, window) =>
      AccumulationPhase(inner, window, NumericAccumulatedState(window))({ case a: NumericAccumulatedState => a.count }, "count"))

    lazy val numericParser: Parser[PhaseParser[Event, _, Double]] = numberParser | field | windowFunction

    lazy val comparator: Parser[String] = space.*() ~> """(<|<=|>|=>|!=|==)""".r <~ space.*()

    //todo
    lazy val numericComparatorParser: Parser[PhaseParser[Event, _, Boolean]] =
      (numericParser ~ comparator ~ numericParser) ^^ ((_, left, comparator, right) => GreaterParser(left, right))

    lazy val binaryBoolean: Parser[String] = space.*() ~> """and|or|&|\|""".r <~ space.*()

    //todo
    lazy val binaryComparatorParser: Parser[PhaseParser[Event, _, Boolean]] =
      booleanParser ~ binaryBoolean ~ booleanParser ^^ ((_, left, op, right) => AndParser(left, right))

    lazy val unaryBoolean: Parser[String] = space.*() ~> """!|not|assert|wait""".r <~ space.*()

    //todo
    lazy val unaryComparator = unaryBoolean ~ space.+() ~ booleanParser ^^ ((_, op, _, inner) => NotParser(inner))

    lazy val booleanParser: Parser[PhaseParser[Event, _, Boolean]] =
      ("(" ~ space.+()) ~> booleanParser <~ (space.+() ~ ")") | binaryComparatorParser | unaryComparator | numericComparatorParser

    lazy val timed: Parser[TimeInterval] = ("timed" ~ "(") ~>
      ("<" ~ window ^^ ((_, _, w) => TimeInterval(Time.MinWindow, w)) |
        ">" ~ window ^^ ((_, _, w) => TimeInterval(w, Time.MaxWindow)) |
        window ~ "," ~ window ^^ ((_, min, _, max) => TimeInterval(min, max))
        ) <~ ")"

    lazy val timedParser = parser ~ timed ^^ ((_, inner, timeInterval) => Timed(inner, timeInterval))

    lazy val parser: Parser[PhaseParser[Event, _, _]] =
      ("(" ~ space.*()) ~> parser <~ (space.*() ~ ")") | booleanParser | numericParser | timedParser

    parser
  }
}


//periodName := s | ms | sec | second | min | minute | hour | h | day | d | ms | millisecond
//window := $number $periodName
//functionName := avg | min | max |...- can be set externally
//windowFunction := $functionName ($numericParser in $window)
//
//numericParser := $number | $field | $windowFunction

//сomparator := > | >= | < | <= | == | !=
//numericComparatorParser := $numericParser $comparator $numericParser
//binaryBoolean := and | or | & | \|
//binaryComparatorParser := $booleanPhase $binaryBoolean $booleanPhase
//unaryComparator := ! | not | assert | wait
//unaryComparatorParser := $unaryComparator $booleanPhase
//booleanParser := ($booleanParser) | $binaryComparatorParser | $unaryComparatorParser | $binaryComparatorParser


//time := timed (< $window) | timed (> $window) | timed ($window, $window)
//timedParser := $parser $time

//bracketedParser := ($parser)
//
//parser := $bracketedParser | $booleanParser | $numericParser | $timedParser
//