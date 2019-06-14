package ru.itclover.tsp.core.optimizations
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.optimizations.Pat.Pat

import scala.language.reflectiveCalls

trait WithInner[E, S <: PState[T, S], T, P <: Pattern[E, S, T]] extends Replacable[P, P]

class Optimizer[E: IdxExtractor] {

  def optimizations[T] = Seq(coupleOfTwoSimple[T], coupleOfTwoConst[T], mapOfConst[T], mapOfSimple[T])

  def applyOptimizations[S <: PState[T, S], T](pat: Pattern[E, S, T]): (Pattern[E, S, T], Boolean) = {
    optimizations[T].foldLeft(pat -> false) {
      case ((p, _), rule) if rule.isDefinedAt(Pat(p)) => rule.apply(Pat(p)).asInstanceOf[Pattern[E, S, T]] -> true
      case (x, _)                                     => x
    }
  }

  def optimize[S <: PState[T, S], T](pattern: Pattern[E, S, T]): Pat[E, T] = {

    val optimizedPatternIterator = Iterator.iterate(Pat(pattern) -> true) { case (p, _) => applyOptimizations(p) }

    // try no more than 10 cycles of optimizations to avoid infinite recursive loops in case of wrong rule.
    optimizedPatternIterator.takeWhile(_._2).take(10).map(_._1).toSeq.last
  }

  type OptimizeRule[T] = PartialFunction[Pat[E, T], Pat[E, T]]

  private def coupleOfTwoConst[T]: OptimizeRule[T] = {
    case Pat(x @ CouplePattern(Pat(ConstPattern(a)), Pat(ConstPattern(b)))) =>
      Pat(ConstPattern[E, T](x.func.apply(a, b)))
  }

  private def coupleOfTwoSimple[T]: OptimizeRule[T] = {
    case Pat(x @ CouplePattern(Pat(left @ SimplePattern(fleft)), Pat(right @ SimplePattern(fright)))) =>
      Pat(
        SimplePattern[E, T](
          event => x.func.apply(fleft.apply(event.asInstanceOf[Nothing]), fright.apply(event.asInstanceOf[Nothing]))
        )
      )
  }

  private def mapOfConst[T]: OptimizeRule[T] = {
    case Pat(map @ MapPattern(Pat(ConstPattern(x)))) => Pat(ConstPattern[E, T](x.flatMap(map.func)))
  }

  private def mapOfSimple[T]: OptimizeRule[T] = {
    case Pat(map @ MapPattern(Pat(simple: SimplePattern[E, _]))) =>
      Pat(SimplePattern[E, T](simple.f.andThen(map.func)))
  }

}

object Pat {

  type Pat[E, T] = ({
    type S <: PState[T, S]
    type Inner = Pattern[E, S, T]
  })#Inner

  def apply[E, T](pattern: Pattern[E, _, T]): Pat[E, T] = pattern.asInstanceOf[Pat[E, T]]

  def unapply[E, _, T](arg: Pat[E, T]): Option[Pattern[E, _, T]] = arg match {
    case x: Pattern[E, _, T] => Some(x)
  }
}
