package ru.itclover.tsp.dsl

import org.parboiled2.ParseError
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.TestApp.TestEvent
import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor

import scala.util.{Failure, Success}

class SyntaxTest extends FlatSpec with Matchers with PropertyChecks {
  val validRule = "(x > 1) for 5 seconds andthen (y < 2)"
  val invalidRule = "1 = invalid rule"
  val multiAvgRule = "avg(x, 5 sec) + avg(x, 30 sec) + avg(x, 60 sec) > 300"

  implicit val extractTime: TimeExtractor[TestEvent] = new TimeExtractor[TestEvent] {
    override def apply(v1: TestEvent) = v1.time
  }

  implicit val numberExtractor: SymbolNumberExtractor[TestEvent] = new SymbolNumberExtractor[TestEvent] {
    override def extract(event: TestEvent, symbol: Symbol): Double = 0.0
  }

  val rules = Seq(
    "Speed > 2 and SpeedEngine > 260 and PosKM > 0 and PAirBrakeCylinder > 0.1  for 2 sec",
    "TWaterInChargeAirCooler > 72 for 60 sec",
    "BreakCylinderPressure = 1 and SpeedEngine > 300 and PosKM > 0 and SpeedEngine > 340 for 3 sec",
    "SensorBrakeRelease = 1 and SpeedEngine > 260 and PosKM > 3 and PosKM < 16 and Speed > 2 for 3 sec",
    "(SpeedEngine = 0 for 100 sec and POilPumpOut > 0.1 for 100 sec > 50 sec) andThen SpeedEngine > 0",
    "(lag(SpeedEngine) = 0 and TOilInDiesel < 45 and TOilInDiesel > 8 and (ContactorOilPump = 1 for 7 min < 80 sec)) andThen SpeedEngine > 0",
    "SpeedEngine > 600 and PosKM > 4 and TColdJump > 0 and TColdJump < 80 and abs(avg(TExGasCylinder, 10 sec) - TExGasCylinder) > 100 for 60 sec",
    "Current_V=0 andThen lag(I_OP) =0 and I_OP =1 and Current_V < 15",
    "(PosKM > 4 for 120 min < 60 sec) and SpeedEngine > 0",
    "K31 = 0 and lag(QF1) = 1 and QF1 = 0 and U_Br = 1 and pr_OFF_P7 = 1 and SA7 = 0 and Iheat < 300 and lag(Iheat) < 300",
    "SpeedEngine > 300 and (ContactorOilPump = 1 or ContactorFuelPump = 1) for 5 sec",
    "SpeedEngine>300 for 60 sec andThen PAirMainRes > 7.8 and lag(CurrentCompressorMotor) < 10 and CurrentCompressorMotor >= 10 andThen CurrentCompressorMotor > 100 for 2 sec",
    "SpeedEngine > 260 and PowerPolling >= 50 and (PowerPolling > 136.5 and PosKM = 1 or PowerPolling > 273 and PosKM = 2 or PowerPolling > 462 and PosKM = 3 or PowerPolling > 724.5 and PosKM = 4 or PowerPolling > 997.5 and PosKM = 5 or PowerPolling > 1207.5 and PosKM = 6 or PowerPolling > 1344 and PosKM = 7 or PowerPolling > 1470 and PosKM = 8 or PowerPolling > 1522.5 and PosKM = 9 or PowerPolling > 1680 and PosKM = 10 or PowerPolling > 1827 and PosKM = 11 or PowerPolling > 2058 and PosKM = 12 or PowerPolling > 2152.5 and PosKM = 13 or PowerPolling > 2257.5 and PosKM = 14 or PowerPolling > 2373 and PosKM = 15) for 60 sec",
    "(Uks > 0 and Uks < 150 and (Uks <76 or Uks > 120)) or (Uks > 150 and (Uks<19000 or Uks < 30000)) for 30 sec",
    "K31 = 0 and lag(QF1) = 1 and QF1 = 0 and pr_OFF_P1 = 0 and pr_OFF_P2 = 0 and pr_OFF_P3 = 0 and pr_OFF_P4 = 0 and pr_OFF_P5 = 0 and pr_OFF_P7 = 0 and pr_OFF_P10 = 0 and pr_OFF_P11 = 0",
    "(K7_1 = 1 and WORK1_VPP = 1) or (K7_2 = 1 and WORK2_VPP = 1) andThen A4 = 1 for 60 min",
    "SpeedEngine > 300 and PowerPolling > 50 and PFuelFineFuelFilter < 1.3 and PFuelFineFuelFilter > 0.1 for 10 sec",
    "workmodeRecupPull = 0 and uivoltage > 900 and i1 - lag(i1) > 150",
    "lag(SpeedEngine) > 0 and SpeedEngine = 0 andThen POilPumpOut > 0.1 for 100 sec < 50 sec",
    "SpeedEngine > 300 and TOilOutDiesel > 80 and (PosKM > 3 and PosKM < 15 and POilDieselOut < 1.3 or PosKM = 15 and POilDieselOut < 5.5) for 10 sec",
    "CurrentExcitationGenerator > 5400 for 5 min",
    "SpeedEngine > 260 and (PowerPolling > 70 and PosKM = 1 or " +
    "PowerPolling > 85 and PosKM = 2 or PowerPolling > 170 and PosKM = 3 or " +
    "PowerPolling > 300 and PosKM = 4 or PowerPolling > 445 and PosKM = 5 or " +
    "PowerPolling > 575 and PosKM = 6 or PowerPolling > 720 and PosKM = 7 or " +
    "PowerPolling > 860 and PosKM = 8 or PowerPolling > 1140 and PosKM = 9 or " +
    "PowerPolling > 1000 and PosKM = 10 or PowerPolling > 1180 and PosKM = 11 or " +
    "PowerPolling > 1520 and PosKM = 12 or PowerPolling > 1680 and PosKM = 13 or " +
    "PowerPolling > 1785 and PosKM = 14) for 60 sec",
    "K31 = 0 and KM126 =1 and KM131=0 for 3 sec",
    "lag(SpeedEngine) >0  and SpeedEngine = 0 and Speed < 5 for 60 min",
    "SpeedEngine > 260 and PowerPolling > 50 and " +
    "(PowerPolling < 33 and PosKM = 1 or " +
    "PowerPolling < 47 and PosKM = 2 or " +
    "PowerPolling < 114 and PosKM = 3 or " +
    "PowerPolling < 213 and PosKM = 4 or " +
    "PowerPolling < 351 and PosKM = 5 or " +
    "PowerPolling < 456 and PosKM = 6 or " +
    "PowerPolling < 570 and PosKM = 7 or " +
    "PowerPolling < 684 and PosKM = 8 or " +
    "PowerPolling < 712 and PosKM = 9 or " +
    "PowerPolling < 807 and PosKM = 10 or " +
    "PowerPolling < 940 and PosKM = 11 or " +
    "PowerPolling < 1225 and PosKM = 12 or " +
    "PowerPolling < 1368 and PosKM = 13 or " +
    "PowerPolling < 1548 and PosKM = 14 or " +
    "PowerPolling < 1757 and PosKM = 15) for 60 sec",
    "\"Section\" = 0"
  )

  "Parser" should "parse rule" in {
    val p = new SyntaxParser(validRule)
    p.start.run() shouldBe a[Success[_]]
  }

  "Parser" should "not parse invalid rule" in {
    val p = new SyntaxParser(invalidRule)
    p.start.run() shouldBe a[Failure[_]]
  }

  "Phase builder" should "correctly build rule" in {
    PhaseBuilder.build(multiAvgRule).right.get._1 shouldBe a[Pattern[TestEvent, _, _]]
  }

  forAll(Table("rules", rules: _*)) { r =>
    val p = new SyntaxParser(r)
    val x = p.start.run()
    x shouldBe a[Success[_]]
    x.get shouldBe an[Pattern[TestEvent, _, _]]
  }
}
