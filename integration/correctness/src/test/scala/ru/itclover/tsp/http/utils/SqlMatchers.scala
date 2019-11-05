package ru.itclover.tsp.http.utils

import com.typesafe.scalalogging.Logger
import org.scalactic.Equality
import org.scalatest.{Assertion, Matchers}

trait SqlMatchers extends Matchers {

  val logger = Logger("SqlMatchers")

  /** Util for checking segments count and size in seconds */
  def checkByQuery(expectedValues: Seq[Double], query: String, epsilon: Double = 0.0001)(
    implicit container: JDBCContainer
  ): Assertion = {
    val resultSet = container.executeQuery(query)
    val results = new Iterator[Double] {
      override def hasNext: Boolean = resultSet.next
      override def next(): Double = resultSet.getDouble(1)
    }.toList
    // misleading, but unfortunately lower levels don't work
    logger.error(s"Expected Values: [${expectedValues.mkString(", ")}], actual values: [${expectedValues.mkString(", ")}]")
    implicit val customEquality: Equality[List[Double]] = (a: scala.List[Double], b: Any) => {
      a.zip(b.asInstanceOf[Iterable[Double]]).forall { case (x, y) => Math.abs(x - y) < epsilon }
    }
//    for (expectedVal <- expectedValues) {
//      resultSet.next() shouldEqual true
//      val value = resultSet.getDouble(1)
//      value should === (expectedVal +- epsilon)
//    }
    results should ===(expectedValues)
  }
}
