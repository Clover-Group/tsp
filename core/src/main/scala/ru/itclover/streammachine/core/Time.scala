package ru.itclover.streammachine.core

import java.time.{Duration, Instant}
import java.util.Date

import ru.itclover.streammachine.core.Time.{MaxWindow, MinWindow}

import scala.math.Ordering.Long

trait Time {
  def plus(window: Window): Time

  def toMillis: Long
}

trait Window {
  def toMillis: Long
}

case class TimeInterval(min: Window = MinWindow, max: Window = MaxWindow){
  assert(min.toMillis >= 0 && max.toMillis >= 0 && max.toMillis >= min.toMillis,
    s"Incorrect Timer configuration (min: ${min.toMillis}, max: ${max.toMillis})")
}

object Time {
  type TimeExtractor[Event] = Event => Time

  implicit val timeOrdering: Ordering[Time] = new Ordering[Time] {
    override def compare(x: Time, y: Time) = Long.compare(x.toMillis, y.toMillis)
  }

  implicit class durationWindow(val duration: Duration) extends Window {
    override def toMillis: Long = duration.toMillis
  }

  implicit class scalaDurationWindow(val d: scala.concurrent.duration.Duration) extends Window {
    override def toMillis: Long = d.toMillis
  }

  implicit class longWindow(d: Long) extends Window {
    override def toMillis: Long = d
  }

  implicit class LongTimeLike(t: Long) extends Time {
    override def plus(window: Window): Time = t + window.toMillis

    override def toMillis: Long = t
  }

  implicit class InstantTimeLike(t: Instant) extends Time {
    override def plus(window: Window): Time = t.plusMillis(window.toMillis)

    override def toMillis: Long = t.toEpochMilli
  }

  implicit class javaDateTimeLike(t: Date) extends Time {
    override def plus(window: Window): Time = t.getTime + window.toMillis

    override def toMillis: Long = t.getTime
  }

  object MinWindow extends Window {
    override def toMillis: Long = 0l
  }

  object MaxWindow extends Window {
    override def toMillis: Long = java.lang.Long.MAX_VALUE
  }

  def less(w: Window) = TimeInterval(max = w)

  def more(w: Window) = TimeInterval(min = w)

}
