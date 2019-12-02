package ru.itclover.tsp.http.utils

import com.typesafe.scalalogging.Logger
import org.scalactic.Equality
import org.scalatest.{Assertion, Matchers}

import scala.collection.JavaConverters._
import scala.util.Try

trait SqlMatchers extends Matchers {

  val logger = Logger("SqlMatchers")

  /** Util for checking segments count and size in seconds */
  def checkByQuery(expectedValues: Seq[Seq[Double]], query: String, epsilon: Double = 0.0001)(
    implicit container: JDBCContainer
  ): Assertion = {
    val resultSet = container.executeQuery(query)
    val columnCount = resultSet.getMetaData.getColumnCount
    val results = new Iterator[List[Double]] {
      override def hasNext: Boolean = resultSet.next
      override def next(): List[Double] = (1 to columnCount).map(resultSet.getDouble).toList
    }.toList
    // misleading, but unfortunately lower levels don't work
    logger.info(
      s"Expected Values: [${toStringRepresentation(expectedValues)}], " +
      s"actual values: [${toStringRepresentation(results)}]"
    )
    implicit val customEqualityList: Equality[List[Double]] = (a: scala.List[Double], b: Any) => {
      a.size == b.asInstanceOf[Iterable[Double]].size && a.zip(b.asInstanceOf[Iterable[Double]]).forall {
        case (x, y) => Math.abs(x - y) < epsilon
      }
    }
    implicit val customEqualityTable: Equality[List[List[Double]]] = (a: List[List[Double]], b: Any) => {
      a.size == b.asInstanceOf[Iterable[Double]].size && a.zip(b.asInstanceOf[Iterable[List[Double]]]).forall {
        case (x, y) => customEqualityList.areEqual(x, y)
      }
    }
    val unfound = expectedValues.filter(x => !results.exists(y => customEqualityList.areEqual(x.toList, y)))
    val unexpected = results.filter(x => !expectedValues.exists(y => customEqualityList.areEqual(x.toList, y)))
    withClue(s"Expected but not found: [${toStringRepresentation(unfound)}]; found [${toStringRepresentation(unexpected)}] instead") {
      // results should ===(expectedValues)
      unfound shouldBe empty
      unexpected shouldBe empty
    }
  }

  def checkInfluxByQuery(expectedValues: Seq[Seq[Double]], query: String, epsilon: Double = 0.0001)(
    implicit container: InfluxDBContainer
  ): Assertion = {
    val resultSet = container.executeQuery(query)
    val results: List[Seq[Double]] = resultSet.getResults.get(0).getSeries.asScala
      .map(_.getValues.asScala.map(_.asScala.tail.map(x=>Try(x.toString.toDouble).getOrElse(Double.NaN)).toList).toList)
      .foldLeft(List.empty[Seq[Double]])(_ ++ _)
    logger.info(
      s"Expected Values: [${toStringRepresentation(expectedValues)}], " +
        s"actual values: [${toStringRepresentation(results)}]"
    )
    implicit val customEqualityList: Equality[List[Double]] = (a: scala.List[Double], b: Any) => {
      a.size == b.asInstanceOf[Iterable[Double]].size && a.zip(b.asInstanceOf[Iterable[Double]]).forall {
        case (x, y) => Math.abs(x - y) < epsilon
      }
    }
    implicit val customEqualityTable: Equality[List[List[Double]]] = (a: List[List[Double]], b: Any) => {
      a.size == b.asInstanceOf[Iterable[Double]].size && a.zip(b.asInstanceOf[Iterable[List[Double]]]).forall {
        case (x, y) => customEqualityList.areEqual(x, y)
      }
    }
    val unfound = expectedValues.filter(x => !results.exists(y => customEqualityList.areEqual(x.toList, y)))
    val unexpected = results.filter(x => !expectedValues.exists(y => customEqualityList.areEqual(x.toList, y)))
    withClue(s"Expected but not found: [${toStringRepresentation(unfound)}]; found [${toStringRepresentation(unexpected)}] instead") {
      // results should ===(expectedValues)
      unfound shouldBe empty
      unexpected shouldBe empty
    }
  }

  def toStringRepresentation(data: Seq[Seq[Double]]): String = data.map(_.mkString(", ")).mkString("; ")
}
