package ru.itclover.tsp.v2
import cats.{Foldable, Functor, Group, Monad}
import ru.itclover.tsp.core.Window
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.Extract.{IdxExtractor, Result}
import ru.itclover.tsp.v2.aggregators.{GroupPattern, PreviousValue, TimerPattern, WindowStatistic}

import scala.language.higherKinds

//todo refactor it later on
abstract class Patterns[E: IdxExtractor: TimeExtractor, F[_]: Monad, Cont[_]: Functor: Foldable] {

  type Pat[State <: PState[Out, State], Out] = Pattern[E, Out, State, F, Cont]

  implicit class MapSyntax[S <: PState[T, S], T](pattern: Pat[S, T]) {
    def map[A](f: T => A): MapPattern[E, T, A, S, F, Cont] = new MapPattern(pattern)(f.andThen(Result.succ))
    def flatMap[A](f: T => Result[A]) = new MapPattern(pattern)(f)
  }

  implicit class AndThenSyntax[S <: PState[T, S], T](pattern: Pat[S, T]) {

    def andThen[S2 <: PState[T2, S2], T2](nextPattern: Pat[S2, T2]): AndThenPattern[E, T, T2, S, S2, F, Cont] =
      AndThenPattern(pattern, nextPattern)
  }

  implicit class OrderingPatternSyntax[S <: PState[T, S], T: Ordering](pattern: Pat[S, T]) {

    def lteq[S2 <: PState[T, S2]](second: Pat[S2, T]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].lteq(x, y))

    def gteq[S2 <: PState[T, S2]](second: Pat[S2, T]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].gteq(x, y))

    def lt[S2 <: PState[T, S2]](second: Pat[S2, T]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].lt(x, y))

    def gt[S2 <: PState[T, S2]](second: Pat[S2, T]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].gt(x, y))

    def equiv[S2 <: PState[T, S2]](second: Pat[S2, T]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].equiv(x, y))

    def notEquiv[S2 <: PState[T, S2]](second: Pat[S2, T]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield !implicitly[Ordering[T]].equiv(x, y))

    def max[S2 <: PState[T, S2]](second: Pat[S2, T]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].max(x, y))

    def min[S2 <: PState[T, S2]](second: Pat[S2, T]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Ordering[T]].min(x, y))

    //aliases
    def >=[S2 <: PState[T, S2]](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean, F, Cont] = gteq(second)
    def >[S2 <: PState[T, S2]](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean, F, Cont] = gt(second)
    def <[S2 <: PState[T, S2]](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean, F, Cont] = lt(second)
    def <=[S2 <: PState[T, S2]](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean, F, Cont] = lteq(second)
    def ===[S2 <: PState[T, S2]](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean, F, Cont] = equiv(second)
    def =!=[S2 <: PState[T, S2]](second: Pat[S2, T]): CouplePattern[E, S, S2, T, T, Boolean, F, Cont] = notEquiv(second)

  }

  implicit class GroupPatternSyntax[S <: PState[T, S], T: Group](pattern: Pat[S, T]) {

    def plus[S2 <: PState[T, S2]](second: Pat[S2, T]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Group[T]].combine(x, y))

    def minus[S2 <: PState[T, S2]](second: Pat[S2, T]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield implicitly[Group[T]].remove(x, y))

  }

  implicit class BooleanPatternSyntax[S <: PState[Boolean, S]](pattern: Pat[S, Boolean]) {

    def and[S2 <: PState[Boolean, S2]](second: Pat[S2, Boolean]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield x & y)

    def or[S2 <: PState[Boolean, S2]](second: Pat[S2, Boolean]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield x | y)

    def xor[S2 <: PState[Boolean, S2]](second: Pat[S2, Boolean]) =
      new CouplePattern(pattern, second)((t1, t2) => for (x <- t1; y <- t2) yield x & y)

  }

  def assert[S <: PState[Boolean, S]](inner: Pattern[E, Boolean, S, F, Cont]): MapPattern[E, Boolean, Unit, S, F, Cont] =
    inner.flatMap(innerBool => if (innerBool) Result.succ(()) else Result.fail)

  def field[T](f: E => T): SimplePattern[E, T, F, Cont] = new SimplePattern(f.andThen(Result.succ))

  def const[T](a: T): ConstPattern[E, T, F, Cont] = ConstPattern(a)

  def windowStatistic[T, S <: PState[T, S]](i: Pattern[E, T, S, F, Cont], w: Window): WindowStatistic[E, S, T, F, Cont] =
    WindowStatistic(i, w)

  def truthCount[T, S <: PState[T, S]](inner: Pattern[E, T, S, F, Cont], w: Window) =
    windowStatistic(inner, w).map(wsr => wsr.successCount)

  def truthMillis[T, S <: PState[T, S]](inner: Pattern[E, T, S, F, Cont], w: Window) =
    windowStatistic(inner, w).map(wsr => wsr.successMillis)

  def failCount[T, S <: PState[T, S]](inner: Pattern[E, T, S, F, Cont], w: Window) =
    windowStatistic(inner, w).map(wsr => wsr.failCount)

  def failMillis[T, S <: PState[T, S]](inner: Pattern[E, T, S, F, Cont], w: Window) =
    windowStatistic(inner, w).map(wsr => wsr.failMillis)

  def lag[T, S <: PState[T, S]](inner: Pattern[E, T, S, F, Cont], w: Window) = PreviousValue(inner, w)

  def timer[T, S <: PState[T, S]](inner: Pattern[E, T, S, F, Cont], w: Window) = TimerPattern(inner, w)

  def sum[T: Group, S <: PState[T, S]](inner: Pattern[E, T, S, F, Cont], w: Window) = GroupPattern(inner, w).map(_.sum)

  // TODO: Can the count be > Int.MaxValue (i.e. 2^31)?
  def avg[T: Group, S <: PState[T, S]](inner: Pattern[E, T, S, F, Cont], w: Window)(implicit f: Fractional[T]) =
    GroupPattern(inner, w).map(x => f.div(x.sum, f.fromInt(x.count.toInt)))

//  abs(lag(x) - x) > 0 for 10m
}
