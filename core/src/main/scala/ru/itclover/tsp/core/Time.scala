package ru.itclover.tsp.core

import java.math.BigInteger
import java.sql.Timestamp
import java.time.{Duration, Instant}
import java.util.Date
import ru.itclover.tsp.core.Time.{MaxWindow, MinWindow}
import scala.math.Ordering.Long
import org.joda.time.DateTime
import java.text.SimpleDateFormat
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import scala.language.implicitConversions

case class Time(toMillis: Long) extends Serializable {
  def plus(window: Window): Time = Time(toMillis + window.toMillis)

  override def toString: String = Time.DATE_TIME_FORMAT.print(toMillis)

}

case class Window(toMillis: Long) extends Serializable

object Time {

  trait TimeExtractor[Event] extends (Event => Time) with Serializable

  implicit val timeOrdering: Ordering[Time] = new Ordering[Time] {
    override def compare(x: Time, y: Time) = Long.compare(x.toMillis, y.toMillis)
  }

  val DATE_TIME_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  implicit def durationWindow(duration: Duration): Window = Window(toMillis = duration.toMillis)

  implicit def scalaDurationWindow(d: scala.concurrent.duration.Duration): Window = Window(toMillis = d.toMillis)

  implicit def floatWindow(d: Float): Window = Window(toMillis = Math.round(d * 1000))

  implicit def doubleWindow(d: Double): Window = Window(toMillis = Math.round(d * 1000))

  implicit def bigIntWindow(d: BigInteger): Window = Window(toMillis = d.longValue())

  implicit def longWindow(d: Long): Window = Window(toMillis = d)

  implicit def BigIntTimeLike(t: BigInteger): Time = Time(toMillis = t.longValue())

  implicit def DoubleTimeLike(t: Double): Time = Time(toMillis = Math.round(t * 1000.0))

  implicit def FloatTimeLike(t: Float): Time = Time(toMillis = Math.round(t * 1000.0))

  implicit def LongTimeLike(t: Long): Time = Time(toMillis = t)

  implicit def InstantTimeLike(t: Instant): Time = Time(toMillis = t.toEpochMilli)

  implicit def javaDateTimeLike(t: Date): Time = Time(toMillis = t.getTime)

  implicit def jodaDateTimeLike(t: DateTime): Time = Time(toMillis = t.getMillis)

  implicit def javaSqlTimestampLike(t: Timestamp): Time = Time(toMillis = t.getTime)

  object MinWindow extends Window(toMillis = 0l)

  object MaxWindow extends Window(toMillis = java.lang.Long.MAX_VALUE)

  def less(w: Window) = Intervals.TimeInterval(max = w)

  def more(w: Window) = Intervals.TimeInterval(min = w)

}
