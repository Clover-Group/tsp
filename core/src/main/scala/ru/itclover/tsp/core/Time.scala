package ru.itclover.tsp.core

import java.math.BigInteger
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId}

import scala.math.Ordering.Long

case class Time(toMillis: Long) extends Serializable {
  def plus(window: Window): Time = Time(toMillis + window.toMillis)
  def minus(window: Window): Time = Time(toMillis - window.toMillis)

  override def toString: String = Time.DATE_TIME_FORMAT.format(Instant.ofEpochMilli(toMillis))

}

case class Window(toMillis: Long) extends Serializable

object Time {

  implicit val timeOrdering: Ordering[Time] = new Ordering[Time] {
    override def compare(x: Time, y: Time) = Long.compare(x.toMillis, y.toMillis)
  }

  val DATE_TIME_FORMAT: DateTimeFormatter =
    DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.of("UTC"))

  implicit def durationWindow(duration: Duration): Window = Window(toMillis = duration.toMillis)

  implicit def scalaDurationWindow(d: scala.concurrent.duration.Duration): Window = Window(toMillis = d.toMillis)

  implicit def floatWindow(d: Float): Window = Window(toMillis = Math.round(d.toDouble * 1000))

  implicit def doubleWindow(d: Double): Window = Window(toMillis = Math.round(d * 1000))

  implicit def bigIntWindow(d: BigInteger): Window = Window(toMillis = d.longValue())

  implicit def longWindow(d: Long): Window = Window(toMillis = d)

  val MinWindow = Window(toMillis = 0L)

  val MaxWindow = Window(toMillis = java.lang.Long.MAX_VALUE)

  def less(w: Window) = Intervals.TimeInterval(max = w)

  def more(w: Window) = Intervals.TimeInterval(min = w)

}
