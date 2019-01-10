package ru.itclover.tsp.utils

import java.time.Instant

import scala.concurrent.duration.{Duration, _}
import scala.util.Random


trait TimeSeriesGenerator[T] extends PartialFunction[Duration, T] {

  def map[B](f: T => B): TimeSeriesGenerator[B] = MapTimeSeriesGenerator(this.andThen(f))

  def flatMap[B](f: T => TimeSeriesGenerator[B]): TimeSeriesGenerator[B] = FlatMapTimeSeriesGenerator(this, f)

  def after(k: TimeSeriesGenerator[T]): TimeSeriesGenerator[T] = AndThen(this, k)

  def timed(duration: Duration): TimeSeriesGenerator[T] = Timed(this, duration)

  def run(seconds: Int): Seq[T] = (0 to seconds).map(_.seconds).map(this)

  override def isDefinedAt(x: Duration) = x >= 0.seconds && x <= howLong

  def howLong: Duration
}

case class MapTimeSeriesGenerator[T](f: Duration => T) extends TimeSeriesGenerator[T] {

  override def apply(v1: Duration) = f(v1)

  override def howLong = Duration.Inf
}


case class FlatMapTimeSeriesGenerator[R1, R2](parent: TimeSeriesGenerator[R1], f: R1 => TimeSeriesGenerator[R2]) extends TimeSeriesGenerator[R2] {

  override def apply(v1: Duration) = f(parent(v1))(v1)

  override def howLong = parent.howLong
}


case class Constant[T](v: T) extends TimeSeriesGenerator[T] {
  override def apply(v1: Duration) = v

  override def howLong = Duration.Inf
}

case class Change(from: Double, to: Double, howLong: Duration) extends TimeSeriesGenerator[Double] {

  override def apply(v1: Duration) = {
   from +  (to - from) / (howLong.toMillis) * v1.toMillis
  }
}

case class AndThen[A](first: TimeSeriesGenerator[A], next: TimeSeriesGenerator[A]) extends TimeSeriesGenerator[A] {
  override def howLong = first.howLong + next.howLong

  override def apply(v1: Duration) = {
    if (first.isDefinedAt(v1)) first(v1)
    else (next(v1))
  }
}

case class Timed[T](inner: TimeSeriesGenerator[T], dur: Duration) extends TimeSeriesGenerator[T] {

  override def apply(v1: Duration) = inner.apply(v1)

  override def howLong = dur
}

case class Timer(from: Instant) extends TimeSeriesGenerator[Instant] {

  override def apply(v1: Duration) = from.plusMillis(v1.toMillis)

  override def howLong = Duration.Inf
}

case object Milliseconds extends TimeSeriesGenerator[Long] {

  override def apply(v1: Duration) = v1.toMillis

  override def howLong = Duration.Inf
}


case class RandomInRange(from: Int, to: Int)(implicit random: Random) extends TimeSeriesGenerator[Int] {
  override def apply(v1: Duration): Int = random.nextInt(to - from + 1) + from

  override def howLong = Duration.Inf
}