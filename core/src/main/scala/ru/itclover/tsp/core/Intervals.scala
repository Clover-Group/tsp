package ru.itclover.tsp.core
import ru.itclover.tsp.core.Time.{MaxWindow, MinWindow}

object Intervals {

  /** Interval abstraction for time measurment in accumulators and some other patterns */
  sealed trait Interval[T] {
    def contains(item: T) = getRelativePosition(item) == Inside

    def isInfinite: Boolean

    def getRelativePosition(item: T): IntervalPosition
  }

  /** ADT for checking position of item relative to interval */
  sealed trait IntervalPosition extends Product with Serializable

  final case object LessThanBegin extends IntervalPosition
  final case object GreaterThanEnd extends IntervalPosition
  final case object Inside extends IntervalPosition

  /** Inclusive-exclusive interval of time */
  case class TimeInterval(min: Long, max: Long) extends Interval[Long] {
    assert(
      min >= 0 && max >= 0 && max >= min,
      s"Incorrect Timer configuration (min: $min, max: $max)"
    )

    override def contains(w: Long): Boolean = w >= min && w <= max

    override def isInfinite = max == MaxWindow.toMillis

    override def getRelativePosition(item: Long) = {
      if (item < min) {
        LessThanBegin
      } else if (item >= max) {
        GreaterThanEnd
      } else {
        Inside
      }
    }

    def midpoint: Long = (min + max) / 2
  }

  object TimeInterval {
    def apply(min: Window = MinWindow, max: Window = MaxWindow): TimeInterval = TimeInterval(min.toMillis, max.toMillis)

    val MaxInterval = TimeInterval(MaxWindow, MaxWindow)
  }

  /** Simple inclusive-exclusive numeric interval */
  case class NumericInterval[T](start: T, end: Option[T])(implicit numeric: Numeric[T]) extends Interval[T] {

    override def contains(item: T) = numeric.gteq(item, start) && (end.isEmpty || numeric.lteq(item, end.get))

    override def isInfinite: Boolean = end.isEmpty

    override def getRelativePosition(item: T): IntervalPosition = {
      if (numeric.lt(item, start)) {
        LessThanBegin
      } else if (end.isDefined && numeric.gteq(item, end.get)) {
        GreaterThanEnd
      } else {
        Inside
      }
    }
  }

  object NumericInterval {
    def more[T: Numeric](start: T) = NumericInterval(start, None)
    def less[T: Numeric](end: T) = NumericInterval(implicitly[Numeric[T]].zero, Some(end))
  }
}
