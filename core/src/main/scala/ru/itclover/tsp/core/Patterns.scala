package ru.itclover.tsp.core
import cats.Group
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.aggregators._
import ru.itclover.tsp.core.io.TimeExtractor

//todo refactor it later on
abstract class Patterns[E: IdxExtractor: TimeExtractor] {

  type Pat[State, Out] = Pattern[E, State, Out]

  implicit class MapSyntax[S, T](pattern: Pat[S, T]) {
    def map[A](f: T => A): MapPattern[E, T, A, S] = MapPattern(pattern)(f.andThen(Result.succ))
    def flatMap[A](f: T => Result[A]): MapPattern[E, T, A, S] = MapPattern(pattern)(f)
  }

  implicit class AndThenSyntax[S, T](pattern: Pat[S, T]) {

    def andThen[S2, T2](nextPattern: Pat[S2, T2]): AndThenPattern[E, T, T2, S, S2] = AndThenPattern(pattern, nextPattern)
  }

  implicit class OrderingPatternSyntax[S, T: Ordering](pattern: Pat[S, T]) {

    def lteq[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].lteq(x, y))

    def gteq[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].gteq(x, y))

    def lt[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].lt(x, y))

    def gt[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].gt(x, y))

    def equiv[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].equiv(x, y))

    def notEquiv[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield !implicitly[Ordering[T]].equiv(x, y))

    def max[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, T] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].max(x, y))

    def min[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, T] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].min(x, y))

    //aliases
    def >=[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] = gteq(second)
    def >[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] = gt(second)
    def <[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] = lt(second)
    def <=[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] = lteq(second)
    def ===[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] = equiv(second)
    def =!=[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean] = notEquiv(second)

  }

  implicit class GroupPatternSyntax[S, T: Group](pattern: Pat[S, T]) {

    def plus[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, T] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Group[T]].combine(x, y))

    def minus[S2](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, T] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Group[T]].remove(x, y))

  }

  implicit class BooleanPatternSyntax[S](pattern: Pat[S, Boolean]) {

    def and[S2](second: Pat[S2, Boolean]): CouplePattern[E, S, S2, Boolean, Boolean, Boolean] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield x & y)

    def or[S2](second: Pat[S2, Boolean]): CouplePattern[E, S, S2, Boolean, Boolean, Boolean] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield x | y)

    def xor[S2](second: Pat[S2, Boolean]): CouplePattern[E, S, S2, Boolean, Boolean, Boolean] =
      CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield x ^ y)

  }

  def assert[S](inner: Pattern[E, S, Boolean]): MapPattern[E, Boolean, Unit, S] =
    inner.flatMap(innerBool => if (innerBool) Result.succUnit else Result.fail)

  def field[T](f: E => T): SimplePattern[E, T] = SimplePattern(f.andThen(Result.succ))

  def const[T](a: T): ConstPattern[E, T] = ConstPattern(Result.succ(a))

  def windowStatistic[T, S](i: Pattern[E, S, T], w: Window): WindowStatistic[E, S, T] =
    WindowStatistic(i, w)

  def truthCount[T, S](inner: Pattern[E, S, T], w: Window): MapPattern[E, WindowStatisticResult, Long, AggregatorPState[
    S,
    WindowStatisticAccumState[T],
    WindowStatisticResult
  ]] = windowStatistic(inner, w).map(wsr => wsr.successCount)

  def truthMillis[T, S](inner: Pattern[E, S, T], w: Window): MapPattern[E, WindowStatisticResult, Long, AggregatorPState[
    S,
    WindowStatisticAccumState[T],
    WindowStatisticResult
  ]] = windowStatistic(inner, w).map(wsr => wsr.successMillis)

  def failCount[T, S](inner: Pattern[E, S, T], w: Window): MapPattern[E, WindowStatisticResult, Long, AggregatorPState[
    S,
    WindowStatisticAccumState[T],
    WindowStatisticResult
  ]] = windowStatistic(inner, w).map(wsr => wsr.failCount)

  def failMillis[T, S](inner: Pattern[E, S, T], w: Window): MapPattern[E, WindowStatisticResult, Long, AggregatorPState[
    S,
    WindowStatisticAccumState[T],
    WindowStatisticResult
  ]] = windowStatistic(inner, w).map(wsr => wsr.failMillis)

  def timer[T, S](inner: Pattern[E, S, T], w: Window) = TimerPattern(inner, w)

  // tries to join sequential segments together
  // todo add segmentation there it needs
  def segmentizer[T, S](inner: Pattern[E, S, T]) = SegmentizerPattern(inner)

  def lag[T, S](inner: Pattern[E, S, T], w: Window) = SegmentizerPattern(PreviousValue(inner, w))

  def sum[T: Group, S](
    inner: Pattern[E, S, T],
    w: Window
  ): MapPattern[E, GroupAccumResult[T], T, AggregatorPState[S, GroupAccumState[T], GroupAccumResult[T]]] =
    GroupPattern(inner, w).map(_.sum)

  def count[T: Group, S](
    inner: Pattern[E, S, T],
    w: Window
  ): MapPattern[E, GroupAccumResult[T], Long, AggregatorPState[S, GroupAccumState[T], GroupAccumResult[T]]] =
    GroupPattern(inner, w).map(_.count)

  // TODO: Can the count be > Int.MaxValue (i.e. 2^31)?
  def avg[T: Group, S](inner: Pattern[E, S, T], w: Window)(
    implicit f: Fractional[T]
  ): MapPattern[E, GroupAccumResult[T], T, AggregatorPState[S, GroupAccumState[T], GroupAccumResult[T]]] =
    GroupPattern(inner, w).map(x => f.div(x.sum, f.fromInt(x.count.toInt)))
}

object Patterns {

  def apply[E: IdxExtractor: TimeExtractor]: Patterns[E] = new Patterns[E] {}
}
