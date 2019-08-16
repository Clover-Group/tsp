package ru.itclover.tsp.core

import org.scalatest.{Matchers, WordSpec}
import java.time.{Instant, Duration => JavaDuration}

import scala.concurrent.duration.Duration
import java.time.temporal.ChronoUnit
import java.util.Random

import cats.Id
import ru.itclover.tsp.core.Common.{EInt, event, extractor, timeExtractor}
import ru.itclover.tsp.core.Pattern.TsIdxExtractor
import ru.itclover.tsp.utils.{Change, Constant, RandomInRange, Timer}

import scala.collection.mutable.ArrayBuffer
import scala.language.reflectiveCalls

class RulesTest extends WordSpec with Matchers {

  def processTestEvent(ev: Event[Int]): Long = ev.row
  implicit val extractor: TsIdxExtractor[Event[Int]] = new TsIdxExtractor(processTestEvent)

  val testState = SimplePState(PQueue.empty)
  val patterns = new Patterns[Event[Int]] {}
  import patterns._

  val pat = patterns.assert(field(_.row) > const(5))
  val collect = new ArrayBuffer[IdxValue[Int]]()

  "stopWithoutOilPumping" should {

    "match-for-valid-1" in {

      implicit val random: Random = new Random(100)

      val events = (for (time <- Timer(from = Instant.now());
                         pump <- RandomInRange(1, 100)(random)
                           .map(_.toDouble)
                           .timed(
                             Duration.fromNanos(
                               JavaDuration.of(40, ChronoUnit.SECONDS).toNanos
                             )
                           )
                           .after(Constant(0));
                         speed <- Constant(261.0)
                           .timed(
                             Duration.fromNanos(
                               JavaDuration.of(1, ChronoUnit.SECONDS).toNanos
                             )
                           )
                           .after(
                             Change(
                               from = 260.0,
                               to = 0.0,
                               howLong = Duration.fromNanos(
                                 JavaDuration.of(10, ChronoUnit.SECONDS).toNanos
                               )
                             )
                           )
                           .after(Constant(0.0)))
        yield Event[Long](time.getEpochSecond, speed.toInt, pump.toLong)).run(seconds = 100)

      assert(true)
      /*val actState = StateMachine[Id].run(
        pat,
        events.toIterable,
        SimplePState(PQueue.empty),
        (x: IdxValue[Int]) => collect += x
      )
      assert(actState.queue.size === 3)*/

    }

  }

}

object RulesTest extends App {}
