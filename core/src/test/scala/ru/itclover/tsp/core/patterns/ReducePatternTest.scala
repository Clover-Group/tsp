package ru.itclover.tsp.core.patterns

import java.time.Instant

import cats.instances.int._
import cats.{Apply, Id, Semigroup}
import org.scalatest.wordspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Constant, Timer}
import ru.itclover.tsp.core._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

//todo write tests
// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class ReducePatternTest extends AnyWordSpec with Matchers {
  val pat = Patterns[EInt]

  val events = (for (time <- Timer(from = Instant.now());
                     idx  <- Increment;
                     row  <- Constant(0).timed(40.seconds).after(Constant(1)))
    yield Event[Int](time.toEpochMilli, idx.toLong, row, -row)).run(seconds = 100)

  implicit val applicativeResult: Apply[Result] = new Apply[Result] {
    override def ap[A, B](ff: Result[A => B])(fa: Result[A]): Result[B] = ff.flatMap(fa.map)

    override def map[A, B](fa: Result[A])(f: A => B): Result[B] = fa.map(f)
  }
  implicit val semigroup: Semigroup[Result[Int]] = Apply.semigroup[Result, Int]

  "ReducePattern" should {
    "compute result for successes" in {
      import pat._

      // sum values of col and row
      val pattern =
        new ReducePattern(Seq(field(_.row), field(_.col)))(
          func = semigroup.combine,
          (x: Result[Int]) => x,
          (_: Result[Int]) => true,
          Result.succ(0)
        )

      val collect = new ArrayBuffer[IdxValue[Int]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[Int]) => collect += x)

      // returns 1 interval (due to fusing on enqueue)
      collect.size shouldBe 1
      collect(0) shouldBe IdxValue(0, 99, Succ(0))
    }

    "should not produce output if one of inputs does not have output for some interval" in {
      import pat._
      // sum values of row and lagged col
      val pattern =
        new ReducePattern(Seq(lag(field(_.row), 0.seconds), lag(field(_.col), 10.seconds)))(
          func = semigroup.combine,
          (x: Result[Int]) => x,
          (_: Result[Int]) => true,
          Result.succ(0)
        )

      val collect = new ArrayBuffer[IdxValue[Int]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[Int]) => collect += x)

      //returns 2 intervals
      collect.nonEmpty shouldBe true
      collect(0).start shouldBe 10
    }

    //todo more tests!
  }

}
