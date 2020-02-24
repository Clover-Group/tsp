package ru.itclover.tsp.core.utils

import java.time.Instant

import scala.concurrent.duration.{Duration, _}
import scala.util.Random

/**
  *  Common trait for Time Series Generator
  *
  * @tparam T Generic type for generator
  */
trait TimeSeriesGenerator[T] extends PartialFunction[Duration, T] {

  def map[B](f: T => B): TimeSeriesGenerator[B] = MapTimeSeriesGenerator(this.andThen(f))

  def flatMap[B](f: T => TimeSeriesGenerator[B]): TimeSeriesGenerator[B] = FlatMapTimeSeriesGenerator(this, f)

  def after(k: TimeSeriesGenerator[T]): TimeSeriesGenerator[T] = AndThen(this, k)

  def timed(duration: Duration): TimeSeriesGenerator[T] = Timed(this, duration)

  def repeat(count: Int): TimeSeriesGenerator[T] = Repeated(this, count)

  def run(seconds: Int): Seq[T] = (1 to seconds).map(_.seconds).map(this)

  override def isDefinedAt(x: Duration): Boolean = x > 0.seconds && x <= howLong

  def howLong: Duration
}

/**
  * Case class for map of time series
  *
  * @param f method for convert
  * @tparam T type for time series
  */
case class MapTimeSeriesGenerator[T](f: Duration => T) extends TimeSeriesGenerator[T] {

  override def apply(v1: Duration): T = f(v1)

  override def howLong: Duration.Infinite = Duration.Inf
}

/**
  *
  * @param parent - Parent TimeSeriesGenerator for flat map
  * @param f function for convert
  * @tparam R1 type for input function
  * @tparam R2 type for output function
  */
case class FlatMapTimeSeriesGenerator[R1, R2](parent: TimeSeriesGenerator[R1], f: R1 => TimeSeriesGenerator[R2])
    extends TimeSeriesGenerator[R2] {

  override def apply(v1: Duration): R2 = f(parent(v1))(v1)

  override def howLong: Duration = parent.howLong
}

/**
  * Case class with constant for TimeSeries
  *
  * @param v value for constant
  * @tparam T Generic type for generator
  */
case class Constant[T](v: T) extends TimeSeriesGenerator[T] {
  override def apply(v1: Duration): T = v

  override def howLong: Duration.Infinite = Duration.Inf
}

/**
  * Case class for changing Time Series
  *
  * @param from start date
  * @param to end date
  * @param howLong duration of change
  */
case class Change(from: Double, to: Double, howLong: Duration) extends TimeSeriesGenerator[Double] {

  override def apply(v1: Duration): Double = (to - from) / howLong.toMillis * v1.toMillis
}

/**
  * Pattern for continuing time series
  *
  * @param first first generator
  * @param next second generator
  * @tparam A input type for generators
  */
case class AndThen[A](first: TimeSeriesGenerator[A], next: TimeSeriesGenerator[A]) extends TimeSeriesGenerator[A] {
  override def howLong: Duration = first.howLong + next.howLong

  override def apply(v1: Duration): A = {
    if (first.isDefinedAt(v1)) {
      first(v1)
    } else {
      next(v1 - first.howLong)
    }
  }
}

/**
  * Case Class for timing the generator
  *
  * @param inner inner generator
  * @param dur duration of timing
  * @tparam T Generic type for generator
  */
case class Timed[T](inner: TimeSeriesGenerator[T], dur: Duration) extends TimeSeriesGenerator[T] {

  override def apply(v1: Duration): T = inner.apply(v1)

  override def howLong: Duration = dur
}

case class Repeated[T](inner: TimeSeriesGenerator[T], times: Int) extends TimeSeriesGenerator[T] {
  assert(inner.howLong != Duration.Inf)
  override def howLong: Duration = inner.howLong * times

  override def apply(v1: Duration): T = inner.apply(Duration.fromNanos(v1.toNanos % inner.howLong.toNanos))
}

/**
  * Case class for timing(start date)
  *
  * @param from start date
  */
case class Timer(from: Instant) extends TimeSeriesGenerator[Instant] {

  override def apply(v1: Duration): Instant = from.plusMillis(v1.toMillis)

  override def howLong: Duration.Infinite = Duration.Inf
}

/**
  * Case class for generating random values in range
  *
  * @param from start
  * @param to end
  * @param random instance of java.util.Random
  */
case class RandomInRange(from: Int, to: Int)(implicit random: Random) extends TimeSeriesGenerator[Int] {
  override def apply(v1: Duration): Int = random.nextInt(to - from + 1) + from

  override def howLong: Duration.Infinite = Duration.Inf
}

object TimeSeriesGenerator {

  val Increment: TimeSeriesGenerator[Int] =
    Change(0, 1000000000 - 1, howLong = 1000000000.seconds).map((x: Double) => x.toInt)
}
