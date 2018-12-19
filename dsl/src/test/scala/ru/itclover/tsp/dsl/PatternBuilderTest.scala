package ru.itclover.tsp.dsl

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.io.{Decoder, Extractor, TimeExtractor}

import scala.util.{Failure, Success}

case class TestEvent() {

}


class PatternBuilderTest extends FlatSpec with Matchers with PropertyChecks {

  
  val timedRulesAndSensors = Seq(
    ("rule1s >= 8 for 10 min > 9 min", Seq("rule1s")),
    ("rule2s1 = 100 andThen rule2s2 >= 8 for 10 min > 9 min", Seq("rule2s1", "rule2s2")),
    ("rule3s = 1 for 120 sec > 115 times", Seq("rule3s")),
    ("rule4s >= 5990 for 5 min", Seq("rule4s"))
  )

  val timeRangedRules = Seq(
    "rule5s1 > 0 for 27 to 33 seconds",
    "rule5s1 > 0 for 30 seconds +- 3 seconds",
    "rule5s1 > 0 for 30 seconds +- 10%"
  )

  val multiAvgRule = "avg(x, 5 sec) + avg(x, 30 sec) + avg(x, 60 sec) > 300"

  implicit val extractTime: TimeExtractor[TestEvent] = new TimeExtractor[TestEvent] {
    override def apply(v1: TestEvent) = Time(0L)
  }
  
  implicit val iToIDecoder = new Decoder[Int, Int] {
    override def apply(x: Int) = x
  }
  
  implicit val iToDDecoder = new Decoder[Int, Double] {
    override def apply(x: Int) = x.toDouble
  }

  implicit val numberExtractor: Extractor[TestEvent, Int, Int] = new Extractor[TestEvent, Int, Int] {
    
    override def apply[T](e: TestEvent, k: Int)(implicit d: Decoder[Int, T]) = k.asInstanceOf[T]
  }

  "Pattern builder" should "correctly parse used sensors for rules with windows" in {
    timedRulesAndSensors.foreach {
      case (rule, sensors) => {
        new SyntaxParser[TestEvent, Int, Int](rule, SyntaxParser.testFieldsIdxMap).start.run() match {
          case Success(pattern) => PatternBuilder.findFields(pattern) shouldBe sensors
          case Failure(ex)    => fail(ex)
        }
      }
    }
  }

  "Pattern builder" should "correctly determine max time phase" in {
    val p = new SyntaxParser[TestEvent, Int, Int](multiAvgRule, SyntaxParser.testFieldsIdxMap)
    p.start.run() match {
      case Success(pattern) => PatternBuilder.maxPhaseWindowMs(pattern) shouldBe 60000
      case Failure(ex)    => fail(ex)
    }
  }

  "Pattern builder" should "correctly determine window in ranged rules" in {
    timeRangedRules.foreach {
      rule =>
        new SyntaxParser[TestEvent, Int, Int](rule, SyntaxParser.testFieldsIdxMap).start.run() match {
          case Success(pattern) => PatternBuilder.maxPhaseWindowMs(pattern) shouldBe 33000
          case Failure(ex)    => fail(ex)
        }
    }
  }

}
