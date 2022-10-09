package ru.itclover.tsp.http.utils

import com.typesafe.scalalogging.Logger
import org.scalactic.Equality
import org.scalatest.Assertion
import org.scalatest.matchers.should._

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
// Here we also use `asInstanceOf` methods for type conversion.
// We suppress Any for `shouldBe empty` statements.
@SuppressWarnings(
  Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Any")
)
trait SqlMatchers extends Matchers {

  val logger = Logger("SqlMatchers")

  /** Util for checking segments count and size in seconds */
  // Here, default argument for `epsilon` is useful.
  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
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

    val unfound = expectedValues.filter(x => !results.exists(y => customEqualityList.areEqual(x.toList, y)))
    val unexpected = results.filter(x => !expectedValues.exists(y => customEqualityList.areEqual(x.toList, y)))
    withClue(
      s"Expected but not found: [${toStringRepresentation(unfound)}]; found [${toStringRepresentation(unexpected)}] instead"
    ) {
      // results should ===(expectedValues)
      unfound shouldBe empty
      unexpected shouldBe empty
    }
  }

  def toStringRepresentation(data: Seq[Seq[Double]]): String = data.map(_.mkString(", ")).mkString(";\n")
}
