package ru.itclover.tsp.aggregators.accums

import ru.itclover.tsp.core.{Time, Window}

object OneTimeStates {

  case class CountAccumState[T](
    window: Window,
    count: Long = 0L,
    startTime: Option[Time] = None,
    lastTime: Option[Time] = None
  ) extends AccumState[T] {
    override def updated(time: Time, value: T) = {
      CountAccumState(
        window = window,
        count = count + 1,
        startTime = startTime.orElse(Some(time)),
        lastTime = Some(time)
      )
    }
  }

  case class TruthAccumState(
    window: Window,
    truthCount: Long = 0L,
    truthMillisCount: Long = 0L,
    prevValue: Boolean = false,
    startTime: Option[Time] = None,
    lastTime: Option[Time] = None
  ) extends AccumState[Boolean] {

    def updated(time: Time, value: Boolean): TruthAccumState = {
      lastTime match {
        case Some(prevTime) =>
          // If first and prev value is true - add time between it and current value to
          // millis accumulator (or it won't be accounted at all)
          val msToAddForPrevValue = if (prevValue && truthMillisCount == 0L) time.toMillis - prevTime.toMillis else 0L
          val currentTruthMs = if (value) time.toMillis - prevTime.toMillis else 0L
          TruthAccumState(
            window = window,
            truthCount = truthCount + (if (value) 1 else 0),
            truthMillisCount = truthMillisCount + currentTruthMs + msToAddForPrevValue,
            startTime = startTime.orElse(Some(time)),
            lastTime = Some(time),
            prevValue = value
          )
        case None =>
          TruthAccumState(
            window = window,
            truthCount = truthCount + (if (value) 1 else 0),
            truthMillisCount = 0L,
            startTime = startTime.orElse(Some(time)),
            lastTime = Some(time),
            prevValue = value
          )
      }
    }
  }

  case class NumericAccumState(
    window: Window,
    sum: Double = 0d,
    count: Long = 0l,
    startTime: Option[Time] = None,
    lastTime: Option[Time] = None
  ) extends AccumState[Double] {

    def updated(time: Time, value: Double): NumericAccumState = {
      NumericAccumState(
        window = window,
        sum = sum + value,
        count = count + 1,
        startTime = startTime.orElse(Some(time)),
        lastTime = Some(time)
      )
    }

    def avg: Double = {
      assert(count != 0, "Illegal state: avg shouldn't be called on empty state.")
      sum / count
    }
  }

  case class LagState[T](
    window: Window,
    value: Option[T] = None,
    startTime: Option[Time] = None,
    lastTime: Option[Time] = None
  ) extends AccumState[T] {

    def updated(
      time: Time,
      value: T
    ): AccumState[T] = {
      LagState[T](
        window = window,
        value = if (value == null) Some(value) else None,
        startTime = startTime.orElse(Some(time)),
        lastTime = Some(time)
      )
    }
  }
}
