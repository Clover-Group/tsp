package ru.itclover.tsp.dsl

import java.time.{Instant, ZonedDateTime}
import org.parboiled2.ParseError
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.aggregators.AggregatorPhases.{Skip, ToSegments}
import ru.itclover.tsp.aggregators.accums
import ru.itclover.tsp.aggregators.accums.{AccumState, PushDownAccumInterval}
import ru.itclover.tsp.aggregators.accums.OneTimeStates.TruthAccumState
import ru.itclover.tsp.core.Intervals.{NumericInterval, TimeInterval}
import ru.itclover.tsp.core.Pattern.Functions
import ru.itclover.tsp.core._
import ru.itclover.tsp.patterns.Ops.SymbolToPattern
import ru.itclover.tsp.patterns.Constants.{const => constant}
import ru.itclover.tsp.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.io.DoubleDecoderInstances._
import ru.itclover.tsp.patterns.Booleans.{Assert, ComparingPattern}
import ru.itclover.tsp.patterns.Constants.ConstPattern
import ru.itclover.tsp.patterns.{Constants, NoState}
import ru.itclover.tsp.patterns.Monads.FlatMapParser
import ru.itclover.tsp.patterns.Numerics.BinaryNumericParser
import scala.collection.immutable.NumericRange
import scala.util.{Failure, Success}

class SyntaxTest extends FlatSpec with Matchers with PropertyChecks {
  val validRule = "(x > 1) for 5 seconds andthen (y < 2)"
  val invalidRule = "1 = invalid rule"
  val multiAvgRule = "avg(x, 5 sec) + avg(x, 30 sec) + avg(x, 60 sec) > 300"

  def equalParser[State1, State2, T](
    left: Pattern[DblTestEvent, State1, T],
    right: Pattern[DblTestEvent, State2, T]
  ): ComparingPattern[DblTestEvent, State1, State2, T] = ComparingPattern(left, right)((a, b) => a equals b, "==")

  def const[T](value: T) = constant[DblTestEvent, T](value)

  implicit val extractor: Extractor[DblTestEvent, Symbol, Double] = new Extractor[DblTestEvent, Symbol, Double] {
    override def apply[T](e: DblTestEvent, k: Symbol)(implicit d: Decoder[Double, T]) = e.result match {
      case PatternResult.Success(x) => x
      case x => sys.error(s"Invalid test pattern, cannot access non-successes result - $x")
    }
  }

  val rules: Seq[(String, Pattern[DblTestEvent, _, _])] = Seq(
    (
      "Speed > 2 and SpeedEngine > 260 and PosKM > 0 and PAirBrakeCylinder > 0.1  for 2 sec",
      ToSegments(
        Assert(
          'Speed.asDouble > const(2.0)
          and 'SpeedEngine.as[Double] > const(260.0)
          and 'PosKM.as[Double] > const(0.0)
          and 'PAirBrakeCylinder.as[Double] > const(0.1)
        ).timed(Window(2000), Window(2000))
      )
    ),
    (
      "TWaterInChargeAirCooler > 72 for 60 sec",
      ToSegments(
        Assert(
          'TWaterInChargeAirCooler.as[Double] > const(72.0)
        ).timed(Window(60000), Window(60000))
      )
    ),
    (
      "BreakCylinderPressure = 1 and SpeedEngine > 300 and PosKM > 0 and SpeedEngine > 340 for 3 sec",
      ToSegments(
        Assert(
          equalParser('BreakCylinderPressure.as[Double], const(1.0))
          and 'SpeedEngine.as[Double] > const(300.0)
          and 'PosKM.as[Double] > const(0.0)
          and 'SpeedEngine.as[Double] > const(340.0)
        ).timed(Window(3000), Window(3000))
      )
    ),
    (
      "SensorBrakeRelease = 1 and SpeedEngine > 260 and PosKM > 3 and PosKM < 16 and Speed > 2 for 3 sec",
      ToSegments(
        Assert(
          equalParser('SensorBrakeRelease.as[Double], const(1.0))
          and 'SpeedEngine.as[Double] > const(260.0)
          and 'PosKM.as[Double] > const(3.0)
          and 'PosKM.as[Double] < const(16.0)
          and 'Speed.as[Double] > const(2.0)
        ).timed(Window(3000), Window(3000))
      )
    ),
    (
      "(SpeedEngine = 0 for 100 sec and POilPumpOut > 0.1 for 100 sec > 50 sec) andThen SpeedEngine > 0",
      ToSegments(
        Assert(equalParser('SpeedEngine.as[Double], const(0.0)))
          .timed(Window(100000), Window(100000))
        togetherWith
        PushDownAccumInterval(
          Functions
            .truthMillisCount('POilPumpOut.as[Double] > const(0.1), Window(100000)),
          TimeInterval(Window(0), Window(50000))
        ).flatMap(
          msCount => const(msCount > 50000)
        )
        andThen Skip(1, Assert('SpeedEngine.as[Double] > const(0.0)))
      )
    ),
    (
      "(SpeedEngine = 0 for 100 sec and POilPumpOut > 0.1 for exactly 100 sec > 50 sec) andThen SpeedEngine > 0",
      ToSegments(
        Assert(equalParser('SpeedEngine.as[Double], const(0.0)))
          .timed(Window(100000), Window(100000))
        togetherWith
        Functions
          .truthMillisCount('POilPumpOut.as[Double] > const(0.1), Window(100000))
          .flatMap(
            msCount => const(msCount > 50000)
          )
        andThen Skip(1, Assert('SpeedEngine.as[Double] > const(0.0)))
      )
    ),
    (
      "(lag(SpeedEngine) = 0 and TOilInDiesel < 45 and TOilInDiesel > 8 and (ContactorOilPump = 1 for 7 min < 80 sec)) andThen SpeedEngine > 0",
      ToSegments(
        Assert(
          equalParser(Functions.lag('SpeedEngine.as[Double]), const(0.0))
          and 'TOilInDiesel.as[Double] < const(45.0)
          and 'TOilInDiesel.as[Double] > const(8.0)
        )
        togetherWith
        PushDownAccumInterval(
          Functions
            .truthMillisCount(
              equalParser('ContactorOilPump.as[Double], const(1)),
              Window(420000)
            ),
          TimeInterval(Window(0), Window(80000))
        ).flatMap(
          msCount => const(msCount < 80000)
        )
        andThen Skip(1, Assert('SpeedEngine.as[Double] > const(0.0)))
      )
    ),
    (
      "SpeedEngine > 600 and PosKM > 4 and TColdJump > 0 and TColdJump < 80 and abs(avg(TExGasCylinder, 10 sec) - TExGasCylinder) > 100 for 60 sec",
      ToSegments(
        Assert(
          'SpeedEngine.as[Double] > const(600.0)
          and 'PosKM.as[Double] > const(4.0)
          and 'TColdJump.as[Double] > const(0.0)
          and 'TColdJump.as[Double] < const(80.0)
          and Functions.abs(
            BinaryNumericParser(
              Functions.avg(
                'TExGasCylinder.as[Double],
                Window(10000)
              ),
              'TExGasCylinder.as[Double],
              (averaged: Double, actual: Double) => averaged - actual,
              "-"
            )
          ) > const(100.0)
        ).timed(Window(60000), Window(60000))
      )
    ),
    (
      "Current_V=0 andThen lag(I_OP) =0 and I_OP =1 and Current_V < 15",
      ToSegments(
        Assert(equalParser('Current_V.as[Double], const(0)))
        andThen Skip(
          1,
          Assert(
            equalParser(Functions.lag('I_OP.as[Double]), const(0))
            and equalParser('I_OP.as[Double], const(1))
            and 'Current_V.as[Double] < const(15)
          )
        )
      )
    ),
    (
      "(PosKM > 4 for 120 min < 60 sec) and SpeedEngine > 0",
      ToSegments(
        accums
          .PushDownAccumInterval(
            Functions
              .truthMillisCount(
                'PosKM.as[Double] > const(4),
                Window(7200000)
              ),
            TimeInterval(Window(0), Window(60000))
          )
          .flatMap(
            msCount => const(msCount < 60000)
          )
        togetherWith Assert('SpeedEngine.as[Double] > const(0))
      )
    ),
    (
      "K31 = 0 and lag(QF1) = 1 and QF1 = 0 and U_Br = 1 and pr_OFF_P7 = 1 and SA7 = 0 and Iheat < 300 and lag(Iheat) < 300",
      ToSegments(
        Assert(
          equalParser('K31.as[Double], const(0))
          and equalParser(Functions.lag('QF1.as[Double]), const(1))
          and equalParser('QF1.as[Double], const(0))
          and equalParser('U_Br.as[Double], const(1))
          and equalParser('pr_OFF_P7.as[Double], const(1))
          and equalParser('SA7.as[Double], const(0))
          and 'Iheat.as[Double] < const(300)
          and Functions.lag('Iheat.as[Double]) < ConstPattern(300)
        )
      )
    ),
    (
      "SpeedEngine > 300 and (ContactorOilPump = 1 or ContactorFuelPump = 1) for 5 sec",
      ToSegments(
        Assert(
          'SpeedEngine.as[Double] > const(300.0)
          and (
            equalParser('ContactorOilPump.as[Double], const(1))
            or equalParser('ContactorFuelPump.as[Double], const(1))
          )
        ).timed(Window(5000), Window(5000))
      )
    ),
    (
      "\"Section\" = 0",
      ToSegments(Assert(equalParser('Section.as[Double], const(0.0))))
    )
  )

  "Parser" should "parse rule" in {
    val parser = new SyntaxParser[TestEvent[Double], Symbol, Double](validRule, SyntaxParser.testFieldsSymbolMap)
    parser.start.run() shouldBe a[Success[_]]
  }

  "Parser" should "not parse invalid rule" in {
    val parser = new SyntaxParser[TestEvent[Double], Symbol, Double](invalidRule, SyntaxParser.testFieldsSymbolMap)
    parser.start.run() shouldBe a[Failure[_]]
  }

  "Phase builder" should "correctly build rule" in {
    PhaseBuilder.build[TestEvent[Double], Symbol, Double](multiAvgRule, SyntaxParser.testFieldsSymbolMap).right.map {
      case (pat, _) => pat shouldBe a[Pattern[DblTestEvent, _, _]] }
  }

  forAll(Table(("rule", "result"), rules: _*)) { (rule, result) =>
    val parser = new SyntaxParser[TestEvent[Double], Symbol, Double](rule, SyntaxParser.testFieldsSymbolMap)
    val processedResult = parser.start.run()
    val event = TestEvent(Success(0.0).asInstanceOf[PatternResult[Double]], ZonedDateTime.now())
    processedResult shouldBe a[Success[_]]
    processedResult.get shouldBe a[Pattern[DblTestEvent, _, _]]
    processedResult.get.format(event) shouldBe result.format(event)
  }
}
