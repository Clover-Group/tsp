package ru.itclover.tsp.utils

import org.scalatest.WordSpec
import ru.itclover.tsp.core.{TestEvent, _}
import ru.itclover.tsp.core.Pattern.Functions._
import ru.itclover.tsp.core.PatternResult.{Failure, Success, TerminalResult}
import ru.itclover.tsp.core.Time._

class BucketizerTest extends WordSpec with ParserMatchers {
  import Bucketizer._

  "Bucketizer" should {
    implicit val straightWeightExtractor = new WeightExtractor[Long] {
      override def apply(item: Long) = item
    }

    "work on simple cases" in {
      val simplest = Seq(1, 2, 3, 100, 5).map(_.toLong)
      val simplestResult = Bucketizer.bucketizeByWeight(simplest, 2)
      simplestResult.length shouldBe 2
      simplestResult.map(_.totalWeight).sorted shouldBe Vector(11L, 100L)
      simplestResult.map(_.items.length).sorted shouldBe Vector(1, 4)

      // ..
      val bigger = Seq(0, 1, 2, 3, 0, 100, 5, 10000, 20, 13, 10, 0, 0).map(_.toLong)
      val biggerResults = Bucketizer.bucketizeByWeight(simplest, 2)
      biggerResults.length shouldBe 2
      biggerResults.map(_.totalWeight).sorted shouldBe Vector(11L, 100L)
      biggerResults.map(_.items.length).sorted shouldBe Vector(1, 4)
    }

    "not break on corner cases" in {
      // ..
    }
  }

}
