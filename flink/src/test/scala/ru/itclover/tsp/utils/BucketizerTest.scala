package ru.itclover.tsp.utils

import org.scalatest.WordSpec
import org.scalatest.Matchers

class BucketizerTest extends WordSpec with Matchers {
  import Bucketizer._

  "Bucketizer" should {
    implicit val straightWeightExtractor: WeightExtractor[Long] = new WeightExtractor[Long] {
      override def apply(item: Long): Long = item
    }

    "work on simple cases" in {
      val simplest = Seq(1, 2, 3, 100, 5).map(_.toLong)
      val simplestResult = Bucketizer.bucketizeByWeight(simplest, 2)
      simplestResult.length shouldBe 2
      simplestResult.map(_.totalWeight).sorted shouldBe Vector(11L, 100L)
      simplestResult.map(_.totalWeight).sum shouldBe simplest.sum

      val bigger = Seq(0, 1, 2, 3, 0, 100, 5, 10000, 20, 13, 10, 0, 0).map(_.toLong)
      val biggerResults = Bucketizer.bucketizeByWeight(bigger, 3)
      biggerResults.length shouldBe 3
      biggerResults.map(_.totalWeight).sorted shouldBe Vector(54L, 100L, 10000)
      biggerResults.map(_.totalWeight).sum shouldBe bigger.sum
    }

    "not break on corner cases" in {
      val empty = Seq.empty[Long]
      val emptyResult = Bucketizer.bucketizeByWeight(empty, 2)
      emptyResult.length shouldBe 2
      emptyResult.map(_.totalWeight).sum shouldBe 0L

      val same = Seq(2, 2, 2, 2, 2).map(_.toLong)
      val sameResult = Bucketizer.bucketizeByWeight(same, 2)
      sameResult.length shouldBe 2
      sameResult.map(_.totalWeight).sorted shouldBe Vector(4L, 6L)
      sameResult.map(_.totalWeight).sum shouldBe same.sum

      val one = Seq(2, 2, 2, 2, 2).map(_.toLong)
      val oneResult = Bucketizer.bucketizeByWeight(one, 1)
      oneResult.length shouldBe 1
      oneResult.map(_.totalWeight).sorted shouldBe Vector(10L)
      oneResult.map(_.totalWeight).sum shouldBe one.sum

      val zero = Seq(2, 2, 2, 2, 2).map(_.toLong)
      an[IllegalArgumentException] shouldBe thrownBy(Bucketizer.bucketizeByWeight(zero, 0))
    }
  }

}
