package ru.itclover.tsp.v2.aggregators.accums
import java.time.Instant

import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.{Time, Window}
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.utils._
import ru.itclover.tsp.v2.Pattern.Idx
import ru.itclover.tsp.v2.{Patterns, RowWithIdx, StateMachine}
import ru.itclover.tsp.v2.RowWIthIdxCompanion._
import cats.implicits._

import scala.concurrent.duration.DurationInt

class TimerPatternTest extends WordSpec with Matchers {

//  "TimerPatter " should {
//    "return " in {
//      val newTypesHolder = new Patterns[RowWithIdx, cats.Id, List] {}
//      import newTypesHolder._
//
//      val now = Instant.now
//
//      val rows = (for (v   <- Change(0, 10, 10.seconds).timed(5.seconds);
//                       ts  <- Timer(now);
//                       idx <- Milliseconds) yield RowWithIdx(idx, Time(ts.toEpochMilli), v.toInt)).run(11)
//
//      val pattern = timer(newTypesHolder.assert(field(_.value) >= const(5)), 4.seconds)
//
//      val x = StateMachine.run(pattern, rows, pattern.initialState())
//      x.size should be (3)
//    }
//
//  }
}
