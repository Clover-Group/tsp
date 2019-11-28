package ru.itclover.tsp.core

import org.scalatest.{FlatSpec, Matchers}
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.utils.{Change, Constant, PatternMemoryCheck, Timed}
import Time._
import ru.itclover.tsp.core.aggregators.{GroupPattern, TimestampsAdderPattern, WindowStatistic}
import ru.itclover.tsp.core.fixtures.Event

import scala.concurrent.duration._

class CheckMemoryLeaks extends FlatSpec with Matchers {

  val p = Patterns[EInt]
  import p._

  it should "not have memory leaks (SimplePattern)" in {

    val pattern = field(_.row)

    val generator = Change(from = 0.0, to = 100.0, 100.seconds).after(Constant(1)).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 100L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 100L
  }

  it should "not have memory leaks (CouplePattern)" in {

    val pattern = field(_.row) > const(10)

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 10000L
  }

  it should "not have memory leaks (MapPattern)" in {

    val pattern = field(_.row).map(_ + 10)

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 100L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 100L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 100L
  }

  it should "not have memory leaks (SegmentizerPattern)" in {

    val pattern = segmentizer(field(_.row))

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 10000L
  }

  it should "not have memory leaks (AndThenPattern)" in {

    val pattern = field(_.row).andThen(field(_.row))

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 10000L
  }

  it should "not have memory leaks (TimerPattern)" in {

    val pattern = timer(field(_.row), 10.seconds, 2000L)

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 12000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 12000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 12000L
  }

  it should "not have memory leaks (SkipPattern)" in {

    val pattern = skip(field(_.row), 10.seconds)

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 10000L
  }

  it should "not have memory leaks (PreviousValue)" in {

    val pattern = lag(field(_.row), 10.seconds)

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 10000L
  }

  it should "not have memory leaks (TimestampAdderPattern)" in {

    val pattern = new TimestampsAdderPattern(field(_.row))

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 10000L
  }

  it should "not have memory leaks (GroupPattern)" in {

    import cats.instances.int._
    val pattern = GroupPattern(field(_.row), 10.seconds)

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 2000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 5000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 5000L
  }

  it should "not have memory leaks (WindowStatistic)" in {

    val pattern = WindowStatistic(field(_.row), 10.seconds)

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 2000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 4000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 4000L
  }

  it should "not have memory leaks (ReducePattern)" in {

    val pattern = new ReducePattern[Event[Int], SimplePState.type, Int, Int](Seq(field(_.row), field(_.row)))(
      func = (a, b) => a.flatMap(x => b.map(y => x + y)),
      transform = identity,
      filterCond = _.isSuccess,
      initial = Result.succ(123)
    )

    val generator =
      Change(from = 0.0, to = 100.0, 100.seconds).after(Timed(Constant(100.0), 100.seconds)).repeat(1000000).map(_.toInt)

    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 100) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 10000) should be < 10000L
    PatternMemoryCheck.finalStateSizeGenerator(pattern, generator, 1000000) should be < 10000L
  }

}
