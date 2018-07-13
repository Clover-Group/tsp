package ru.itclover.streammachine.newsyntax

import org.parboiled2.ParseError
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class SyntaxTest extends FlatSpec with Matchers with PropertyChecks {
  val validRule = "(x > 1) for 5 seconds andthen (y < 2)"
  val invalidRule = "1 = invalid rule"

  val rules = Seq("(SpeedEngine > 300 AND PosKM = 1 AND PFuelTNVD < 1) OR" +
    "(SpeedEngine > 300 AND PosKM = 15 AND PFuelTNVD < 1.5)" +
    "for 30 sec",
    "CurrentCompressorMotor > 0 and CompressorRelayContact <> 0 and" +
      "((derivation(PAirMainRes) > 0 and PAriMainRes >= 7.5) andThen (PAirMainRes < 8) for 23 sec)",
    "SpeedEngine > 0 and (PosKM > 0 for 120 min < 60 sec)",
    "(Derivation(inKA12_off) <> 0 and inKA12_off = 0) for 12 min >= 3 times",
    "(PosKM > 4 and SpeedEngine > 300) andThen (true for 30 sec) andThen" +
      "((TExGasCylinder1Left >= 100 and TExGasCylinder1Left <= 300) for 1 min)"
  )

  "Parser" should "parse rule" in {
    val p = new SyntaxParser(validRule)
    p.start.run() shouldBe a[Success[_]]
  }

  "Parser" should "not parse invalid rule" in {
    val p = new SyntaxParser(invalidRule)
    p.start.run() shouldBe a[Failure[_]]
  }

  forAll (Table("rules", rules: _*)) {
    r =>
    val p = new SyntaxParser(r)
    p.start.run() shouldBe a[Success[_]]
  }
}
