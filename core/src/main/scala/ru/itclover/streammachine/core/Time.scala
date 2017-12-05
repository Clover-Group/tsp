package ru.itclover.streammachine.core

import java.time.{Duration, Instant}

import scala.math.Ordering.LongOrdering

object TimeImplicits extends TimeLike.Implicits {

}

trait TimeLike[T] extends Ordering[T] {
  def plus[D: DurationLike](t: T, duration: D): T
}

object TimeLike {


  trait Implicits extends DurationLike.Implicits {

    implicit def timeLikeSyntax[T: TimeLike](t: T) = new Object {
      def plus[D: DurationLike](d: D): T = implicitly[TimeLike[T]].plus(t, d)
    }

    implicit object LongTimeLike extends TimeLike[Long] with LongOrdering {
      def plus[D: DurationLike](l: Long, duration: D): Long = l + durationLikeSyntax(duration).toMillis
    }

    implicit object InstantTimeLike extends TimeLike[Instant] {
      def plus[D: DurationLike](i: Instant, duration: D): Instant = i.plusMillis(durationLikeSyntax(duration).toMillis)

      def compare(x: Instant, y: Instant): Int = x.compareTo(y)
    }

    implicit object javaDateTimeLike extends TimeLike[java.util.Date] {
      def plus[D: DurationLike](d: java.util.Date, duration: D): java.util.Date =
        new java.util.Date(d.getTime + durationLikeSyntax(duration).toMillis)

      def compare(x: java.util.Date, y: java.util.Date): Int = x.compareTo(y)
    }

  }

}

trait DurationLike[D] {
  def toMillis(d: D): Long
}

object DurationLike {

  trait Implicits {

    implicit object durationDurationLike extends DurationLike[Duration] {
      override def toMillis(d: Duration): Long = d.toMillis
    }

    implicit object scalaDurationDurationLike extends DurationLike[scala.concurrent.duration.Duration] {
      override def toMillis(d: scala.concurrent.duration.Duration): Long = d.toMillis
    }

    implicit object longDurationLike extends DurationLike[Long] {
      override def toMillis(d: Long): Long = d
    }

    implicit def durationLikeSyntax[D: DurationLike](d: D) = new Object {
      def toMillis: Long = implicitly[DurationLike[D]].toMillis(d)
    }
  }

}