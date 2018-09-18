package ru.itclover.tsp.dsl

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import ru.itclover.tsp.TestApp.TestEvent
import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor
import scala.util.{Failure, Success}

class PhaseBuilderTest extends FlatSpec with Matchers with PropertyChecks {

  val timedRulesAndSensors = Seq(
    ("rule1s >= 8 for 10 min > 9 min", Seq("rule1s")),
    ("rule2s2 = 100 andThen rule2s2 >= 8 for 10 min > 9 min", Seq("rule2s1", "rule2s2")),
    ("rule3s = 1 for 120 sec > 115 times", Seq("rule3s")),
    ("rule4s >= 5990 for 5 min", Seq("rule4s"))
  )
  val multiAvgRule = "avg(x, 5 sec) + avg(x, 30 sec) + avg(x, 60 sec) > 300"

  implicit val extractTime: TimeExtractor[TestEvent] = new TimeExtractor[TestEvent] {
    override def apply(v1: TestEvent) = v1.time
  }

  implicit val numberExtractor: SymbolNumberExtractor[TestEvent] = new SymbolNumberExtractor[TestEvent] {
    override def extract(event: TestEvent, symbol: Symbol): Double = 0.0
  }

  "Phase builder" should "correctly parse used sensors for rules with windows" in {
    timedRulesAndSensors.foreach {
      case (rule, sensors) => {
        (new SyntaxParser(rule)).start.run() match {
          case Success(phase) => PhaseBuilder.findFields(phase) shouldBe equal(sensors)
          case Failure(ex)    => fail(ex)
        }
      }
    }
  }

  "Phase builder" should "correctly determine max time phase" in {
    val p = new SyntaxParser(multiAvgRule)
    p.start.run() match {
      case Success(phase) => PhaseBuilder.maxPhaseWindowMs(phase) shouldBe 60000
      case Failure(ex)    => fail(ex)
    }
  }

}
