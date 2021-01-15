package ru.itclover.tsp.core.aggregators

import java.time.Instant

import cats.Id
import org.scalatest.{Matchers, WordSpec}
import ru.itclover.tsp.core.Time._
import ru.itclover.tsp.core.fixtures.Common.EInt
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.utils.TimeSeriesGenerator.Increment
import ru.itclover.tsp.core.utils.{Constant, RandomInRange, Timer}
import ru.itclover.tsp.core.{IdxValue, Patterns, StateMachine, Window}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.Random

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class GroupPatternTest extends WordSpec with Matchers {

  "GroupPatternTest" should {
    "calculate sum for successes" in {
      val pat = Patterns[EInt]
      import pat._

      val innerPattern = const(1) // returns Success(1) for any sequence of events

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         row  <- Constant(0))
        yield Event[Int](time.toEpochMilli, idx.toLong, row, 0)).run(seconds = 100)

      import cats.instances.int.catsKernelStdGroupForInt

      val groupPattern = GroupPattern(innerPattern, 10.seconds).map(_.sum)

      val collect = new ArrayBuffer[IdxValue[Int]]()
      StateMachine[Id].run(groupPattern, events, groupPattern.initialState(), (x: IdxValue[Int]) => collect += x)

      // 10 intervals due to fusing on enqueue
      collect.size shouldBe 10
      collect.foreach(x => {
        x.value.isSuccess shouldBe true
        x.value.getOrElse(100) < 11 shouldBe true
      })

    }

    "don't fail if there is only few Fails in a window" in {

      val p = Patterns[EInt]
      import p._

      val pattern = p.assert(p.truthCount(p.assert(p.field(_.row) > p.const(5)), Window(10)) > const(0))

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         row  <- RandomInRange(0, 10)(new Random()).timed(5.seconds))
        yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 5)

      val collect = new ArrayBuffer[IdxValue[_]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => collect += x)

      var counter = 0

      collect.foreach(item =>{
        if(item.value.isFail){
          counter += 1
        }
      })

      counter !== 0

    }

    "fail if all events in window are Fails" in {

      val p = Patterns[EInt]
      import p._

      val pattern = p.assert(p.field(_.row) > const(10))

      val events = (for (time <- Timer(from = Instant.now());
                         idx  <- Increment;
                         row  <- RandomInRange(0, 5)(new Random()).timed(5.seconds))
        yield Event[Int](time.toEpochMilli, idx.toLong, row.toInt, 0)).run(seconds = 5)

      val collect = new ArrayBuffer[IdxValue[_]]()
      StateMachine[Id].run(pattern, events, pattern.initialState(), (x: IdxValue[_]) => collect += x)

      collect.foreach(item =>{
        item.value.isFail shouldBe true
      })

    }

  }

}
