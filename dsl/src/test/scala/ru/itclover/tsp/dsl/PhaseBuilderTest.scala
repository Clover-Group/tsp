package ru.itclover.tsp.dsl

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.PropertyChecks
import ru.itclover.tsp.core.{TestEvent, Time}
import ru.itclover.tsp.io.{Decoder, Extractor, TimeExtractor}
import scala.util.{Failure, Success}



class PhaseBuilderTest extends FlatSpec with Matchers with PropertyChecks {

  
  val timedRulesAndSensors = Seq(
    ("rule1s >= 8 for 10 min > 9 min", Seq("rule1s")),
    ("rule2s1 = 100 andThen rule2s2 >= 8 for 10 min > 9 min", Seq("rule2s1", "rule2s2")),
    ("rule3s = 1 for 120 sec > 115 times", Seq("rule3s")),
    ("rule4s >= 5990 for 5 min", Seq("rule4s"))
  )
  val multiAvgRule = "avg(x, 5 sec) + avg(x, 30 sec) + avg(x, 60 sec) > 300"

  implicit val extractTime: TimeExtractor[TestEvent[Int]] = new TimeExtractor[TestEvent[Int]] {
    override def apply(v1: TestEvent[Int]) = Time(0L)
  }
  
  implicit val iToIDecoder = new Decoder[Int, Int] {
    override def apply(x: Int) = x
  }
  
  implicit val iToDDecoder = new Decoder[Int, Double] {
    override def apply(x: Int) = x.toDouble
  }

  implicit val numberExtractor: Extractor[TestEvent[Int], Int, Int] = new Extractor[TestEvent[Int], Int, Int] {
    
    override def apply[T](e: TestEvent[Int], k: Int)(implicit d: Decoder[Int, T]) = k.asInstanceOf[T]
  }

  "Phase builder" should "correctly parse used sensors for rules with windows" in {
    timedRulesAndSensors.foreach {
      case (rule, sensors) => {
        new SyntaxParser(rule, SyntaxParser.testFieldsIdxMap).start.run() match {
          case Success(phase) => PhaseBuilder.findFields(phase) shouldBe sensors
          case Failure(ex)    => fail(ex)
        }
      }
    }
  }

  "Phase builder" should "correctly determine max time phase" in {
    val p = new SyntaxParser(multiAvgRule, SyntaxParser.testFieldsIdxMap)
    p.start.run() match {
      case Success(phase) => PhaseBuilder.maxPhaseWindowMs(phase) shouldBe 60000
      case Failure(ex)    => fail(ex)
    }
  }

}
