package ru.itclover.streammachine.core

import java.sql.Timestamp
import java.time.{Duration, Instant}
import java.util.Date

import ru.itclover.streammachine.core.Time.{MaxWindow, MinWindow}

import scala.math.Ordering.Long
import org.joda.time.DateTime
import java.text.SimpleDateFormat

import org.joda.time.format.DateTimeFormat

trait Time extends Serializable {
  def plus(window: Window): Time

  // TODO:
  // def plusWindow(window: Window): Time
  // def minusWindow(window: Window): Time

  // def plus(t: Time): Time e.g. Time(toMillis + t.toMillis)
  // def minus(t: Time): Time

  def toMillis: Long

  override def toString: String = toMillis.toString
}

trait Window extends Serializable{
  def toMillis: Long
}

case class TimeInterval(min: Window = MinWindow, max: Window = MaxWindow){
  assert(min.toMillis >= 0 && max.toMillis >= 0 && max.toMillis >= min.toMillis,
    s"Incorrect Timer configuration (min: ${min.toMillis}, max: ${max.toMillis})")
}

object Time {
  trait TimeExtractor[Event] extends (Event => Time) with Serializable

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

  implicit class jodaDateTimeLike(t: DateTime) extends Time {
    override def plus(window: Window): Time = t.plus(window.toMillis)

    override def toMillis: Long = t.getMillis

    override def toString: String = {
      val dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
      dtf.print(t)
    }
  }

  implicit class javaSqlTimestampLike(t: Timestamp) extends Time {
    override def plus(window: Window): Time = new Timestamp(t.getTime + window.toMillis)

    override def toMillis: Long = t.getTime

    override def toString: String = {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      dateFormat.format(t)
    }
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
