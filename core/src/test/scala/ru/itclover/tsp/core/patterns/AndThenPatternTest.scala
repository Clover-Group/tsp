package ru.itclover.tsp.core.patterns

import java.time.Instant

import cats.Id
import cats.implicits._
import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Change, Constant, Timer}
import ru.itclover.tsp.core.{Fail, IdxValue, Pattern, Patterns, Result, StateMachine, Succ, Window}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class AndThenPatternTest extends FlatSpec with Matchers {

  val p: Patterns[EInt] = Patterns[EInt]
  import p._

  it should "combine two simple patterns" in {

    val pattern = p.assert(field(_.row) > const(0)).andThen(p.assert(field(_.col) =!= const(0)))

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Change(from = 0.0, to = 100.0, 100.seconds).after(Constant(1)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 100)

    val out = new ArrayBuffer[IdxValue[_]]()
    StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => out += x, 1)
    out.size shouldBe 100
    out.foreach(_.value.isFail should be(true))
  }

  it should "parse combination of and, andThen in rule" in {

    val pattern = p
      .assert(
        (
          field(_.row) > const(10)
        ).and(
          field(_.ts) > const(1000)
        )
      )
      .andThen(
        p.assert(
          field(_.col) =!= const(0)
        )
      )

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Change(from = 0.0, to = 100.0, 100.seconds).after(Constant(1)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 100)

    val out = new ArrayBuffer[IdxValue[_]]()
    StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => out += x, 1)

    out.size shouldBe 100
    out.foreach(_.value shouldBe 'isFail)

  }

  it should "parse combination of and, andThen in rule-2" in {

    val pattern = p
      .assert(
        (
          field(_.row) > const(10)
        ).and(
          field(_.ts) > const(1000)
        )
      )
      .andThen(
        p.assert(
          field(_.col) === const(0)
        )
      )

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Change(from = 0.0, to = 100.0, 100.seconds).after(Constant(1)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 100)

    val out = new ArrayBuffer[IdxValue[_]]()
    StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => out += x, 1)

    out.size shouldBe 100
    val (fails, successes) = out.splitAt(10)
    fails.foreach(_.value shouldBe 'isFail)
    successes.foreach(_.value shouldBe 'isSuccess)

  }

  it should "parse complex rule" in {
    val pattern1 = p.field(_.row > 50).and(p.truthCount(p.assert(p.field(_.row) > p.const(10)), Window(10)) > p.const(0))

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Constant(51).timed(10.seconds).after(Constant(10)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 11)

    val out = new ArrayBuffer[IdxValue[_]]()
    StateMachine[Id].run(pattern1, events, pattern1.initialState(), (x: IdxValue[_]) => out += x, 1)

    out.size shouldBe 11
    out(10) shouldBe IdxValue(10, 10, Result.succ(false))

  }

  def run[A, S, T](pattern: Pattern[A, S, T], events: Seq[A], groupSize: Long = 1000): ArrayBuffer[IdxValue[T]] = {
    val out = new ArrayBuffer[IdxValue[T]]()
    StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[T]) => out += x, 1000)
    out
  }

  it should "work correctly with TimerPattern - 1" in {
    import ru.itclover.tsp.core.Time._

    val first = p.assert(p.field(_.row > 0))
    val second = p.timer(p.assert(p.field(_.col > 0)), 10.seconds, 1000L)
    val pattern = first.andThen(second)

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Constant(0).timed(5.seconds).after(Constant(1).timed(7.seconds)).after(Constant(0));
                       col  <- Constant(0).timed(6.seconds).after(Constant(1).timed(15.seconds)).after(Constant(0)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row, col)).run(seconds = 100)

    val out = run(pattern, events)
    out.size shouldBe 3
    out(0) shouldBe IdxValue(0, 15, Fail)
    out(1) shouldBe IdxValue(16, 20, Succ((16, 20)))
    out(2) shouldBe IdxValue(21, 99, Fail)
  }

  it should "work correctly with TimerPattern - 2" in {
    import ru.itclover.tsp.core.Time._

    val first = p.timer(p.assert(p.field(_.col > 0)), 10.seconds, 1000L)
    val second = p.assert(p.field(_.row > 0))
    val pattern = first.andThen(second)

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Constant(0).timed(5.seconds).after(Constant(1).timed(7.seconds)).after(Constant(0));
                       col  <- Constant(1).timed(15.seconds).after(Constant(0)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row, col)).run(seconds = 100)

    val out = run(pattern, events)
    out.size shouldBe 3
    out(0) shouldBe IdxValue(0, 9, Fail)
    out(1) shouldBe IdxValue(10, 11, Succ((10, 11)))
    out(2) shouldBe IdxValue(12, 99, Fail)
  }

  it should "work correctly with TimerPattern - 3" in {
    import ru.itclover.tsp.core.Time._

    val first = p.timer(p.assert(p.field(_.col > 0)), 10.seconds, 1000L)
    val second = p.timer(p.assert(p.field(_.row > 0)), 10.seconds, 1000L)
    val pattern = first.andThen(second)

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Constant(0).timed(5.seconds).after(Constant(1).timed(17.seconds)).after(Constant(0));
                       col  <- Constant(1).timed(15.seconds).after(Constant(0)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row, col)).run(seconds = 100)

    val out = run(pattern, events)
    out.size shouldBe 3
    out(0) shouldBe IdxValue(0, 19, Fail)
    out(1) shouldBe IdxValue(20, 21, Succ((20, 21)))
    out(2) shouldBe IdxValue(22, 99, Fail)
  }

  it should "work correctly with TimerPattern - 4 repeated pattern" in {
    import ru.itclover.tsp.core.Time._

    val first = p.timer(p.assert(p.field(_.row > 0)), 10.seconds, 1000L)
    val second = p.timer(p.assert(p.field(_.col > 0)), 10.seconds, 1000L)
    val pattern = first.andThen(second)

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row <- Constant(0)
                         .timed(5.seconds)
                         .after(Constant(1).timed(17.seconds).after(Constant(0).timed(5.seconds)).repeat(100));
                       col <- Constant(1).timed(100.seconds).after(Constant(0)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row, col)).run(seconds = 100)

    val out = run(pattern, events)
    out.size shouldBe 8
    out(0) shouldBe IdxValue(0, 24, Fail)
    out(1) shouldBe IdxValue(25, 31, Succ((25, 31)))
    out(2) shouldBe IdxValue(32, 46, Fail)
    out(3) shouldBe IdxValue(47, 53, Succ((47, 53)))
    out(4) shouldBe IdxValue(54, 68, Fail)
    out(5) shouldBe IdxValue(69, 75, Succ((69, 75)))
    out(6) shouldBe IdxValue(76, 90, Fail)
    out(7) shouldBe IdxValue(91, 97, Succ((91, 97)))
  }

  it should "work correctly with TimerPattern - 6 repeated pattern" in {
    import ru.itclover.tsp.core.Time._

    val first = p.timer(p.assert(p.field(_.row > 0)), 10.seconds, 1000L)
    val second = p.timer(p.assert(p.field(_.col > 0)), 10.seconds, 1000L)
    val pattern = first.andThen(second)

    val events = (for (time <- Timer(from = Instant.now());
                       idx  <- Increment;
                       row  <- Constant(1).timed(100.seconds).after(Constant(0));
                       col <- Constant(0)
                         .timed(5.seconds)
                         .after(Constant(1).timed(17.seconds).after(Constant(0).timed(5.seconds)).repeat(100)))
      yield Event[Int](time.toEpochMilli, idx.toLong, row, col)).run(seconds = 100)

    val firstOut = run(first, events)
    val secondOut = run(second, events)

    val out = run(pattern, events)
    out.size shouldBe 9
    out(0) shouldBe IdxValue(0, 19, Fail)
    out(1) shouldBe IdxValue(20, 21, Succ((20, 21)))
    out(2) shouldBe IdxValue(22, 36, Fail)
    out(3) shouldBe IdxValue(37, 43, Succ((37, 43)))
    out(4) shouldBe IdxValue(44, 58, Fail)
    out(5) shouldBe IdxValue(59, 65, Succ((59, 65)))
    out(6) shouldBe IdxValue(66, 80, Fail)
    out(7) shouldBe IdxValue(81, 87, Succ((81, 87)))
    out(8) shouldBe IdxValue(88, 92, Fail)
  }

  it should "work with real-world examples" in {
    val events = Seq(
      Event[Double](1552860727, 1, 9.53, 51),
      Event[Double](1552860728, 2, 9.53, 51),
      Event[Double](1552860729, 3, 9.53, 51),
      Event[Double](1552860730, 4, 9.53, 51),
      Event[Double](1552860731, 5, 9.53, 51),
      Event[Double](1552860732, 6, 9.48, 51),
      Event[Double](1552860733, 7, 9.48, 51),
      Event[Double](1552860734, 8, 9.49, 51),
      Event[Double](1552860735, 9, 9.49, 52),
      Event[Double](1552860736, 10, 9.49, 52),
      Event[Double](1552860737, 11, 9.52, 52),
      Event[Double](1552860738, 12, 9.52, 52),
      Event[Double](1552176055, 13, -0.12, 0),
      Event[Double](1552176056, 14, -0.12, 0),
      Event[Double](1552176057, 15, -0.13, 0)
    )
    val p: Patterns[Event[Double]] = Patterns[Event[Double]]
    import p._

    val pattern = p.assert(p.field(_.row <= 9.53).and(p.field(_.col == 51))).andThen(p.assert(p.field(_.col == 52)))

    val out = run(pattern, events)

    out.size shouldBe 34
  }

}
