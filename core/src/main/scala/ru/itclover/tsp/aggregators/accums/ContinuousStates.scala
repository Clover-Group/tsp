package ru.itclover.tsp.aggregators.accums

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}
import ru.itclover.tsp.utils.CollectionsOps.MutableQueueOps
import ru.itclover.tsp.core.{Time, Window}

object ContinuousStates {

  trait ContinuousAccumState[T] extends AccumState[T] {
    def queue: m.Queue[(Time, T)]

    override def startTime = queue.headOption.map(_._1)

    override def lastTime = queue.lastOption.map(_._1)
  }

  case class CountAccumState[T](window: Window, count: Long = 0L, queue: m.Queue[(Time, T)] = m.Queue.empty[(Time, T)])
    extends ContinuousAccumState[T] {
    override def updated(time: Time, value: T) = {
      val dropList = queue.dequeueWhile { case (oldTime, _) => oldTime.plus(window) < time }
      queue.enqueue((time, value))
      CountAccumState(
        window = window,
        count = count - dropList.length + 1,
        queue = queue
      )
    }
  }

  case class TruthAccumState(window: Window, truthCount: Long = 0L, queue: m.Queue[(Time, Boolean)] = m.Queue())
       extends ContinuousAccumState[Boolean] {

    def updated(time: Time, value: Boolean): TruthAccumState = {
      val dropList = queue.dequeueWhile { case (oldTime, _) => oldTime.plus(window) < time }
      val droppedTruth = dropList.count { case (_, isTruth) => isTruth }
      queue.enqueue((time, value))
      TruthAccumState(
        window = window,
        truthCount = truthCount - droppedTruth + (if (value) 1 else 0),
        queue = queue
      )
    }

    def truthMillisCount: Long = queue match {
      // if queue contains 2 and more elements
      case m.Queue((aTime, aVal), (bTime, _), _*) => {
        // If first value is true - add time between it and next value to time accumulator
        // (or it won't be accounted in consequent foldLeft)
        val firstGapMs = if (aVal) bTime.toMillis - aTime.toMillis else 0L
        // sum-up millis between neighbour elements
        val (overallMs, _) = queue.foldLeft((firstGapMs, aTime)) {
          case ((sumMs, prevTime), (nextTime, nextVal)) =>
            if (nextVal) (sumMs + nextTime.toMillis - prevTime.toMillis, nextTime)
            else (sumMs, nextTime)
        }
        overallMs
      }

      // if queue contains 1 or 0 elements
      case _ => 0L
    }
  }


  case class NumericAccumState(window: Window, sum: Double = 0d, count: Long = 0l,
                               queue: m.Queue[(Time, Double)] = m.Queue.empty)
       extends ContinuousAccumState[Double] {

    def updated(time: Time, value: Double): NumericAccumState = {
      var droppedSum = 0d  // bottleneck-optimization
      val dropList = queue.dequeueWhile { case (oldTime, dropVal) =>
        val doDrop = oldTime.plus(window) < time
        if (doDrop) droppedSum += dropVal else ()
        doDrop
      }
      queue.enqueue((time, value))
      NumericAccumState(
        window = window,
        sum = sum - droppedSum + value,
        count = count - dropList.length + 1,
        queue = queue
      )
    }

    def avg: Double = {
      assert(count != 0, "Illegal state: avg shouldn't be called on empty state.")
      sum / count
    }
  }

  // TODO@trolley813: transferred from OneTimeStates, make continuous
  case class LagState[T](
    window: Window,
    value: Either[T, Null] = Right(null),
    startTime: Option[Time] = None,
    lastTime: Option[Time] = None
  ) extends AccumState[T] {

    def updated(
      time: Time,
      value: T
    ): AccumState[T] = {
      LagState[T](
        window = window,
        value = if (value == null) Left(value) else Right(null),
        startTime = startTime.orElse(Some(time)),
        lastTime = Some(time)
      )
    }
  }
}
