package ru.itclover.streammachine.http.utils

import org.scalatest.Matchers

trait SqlMatchers extends Matchers {
  /** Util for checking segments count and size in seconds */
  def checkByQuery(expectedValues: Seq[Double], query: String, epsilon: Double = 0.0001)
                  (implicit container: JDBCContainer): Unit = {
    val resultSet = container.executeQuery(query)
    for (expectedVal <- expectedValues) {
      resultSet.next() shouldEqual true
      val value = resultSet.getDouble(1)
      value should === (expectedVal +- epsilon)
    }
  }
}
