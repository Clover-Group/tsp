package ru.itclover.tsp.core.optimizations
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.optimizations.Pat.Pat

import scala.language.reflectiveCalls

trait WithInner[E, S <: PState[T, S], T, P <: Pattern[E, S, T]] extends Replacable[P, P]

class Optimizer[E: IdxExtractor] {

  def optimize[S <: PState[T, S], T](pattern: Pattern[E, S, T]): Pat[E, T] = {

    if (coupleOfTwoConst.isDefinedAt(Pat(pattern))) {
      coupleOfTwoConst(Pat(pattern))
    } else if (mapOfConst.isDefinedAt(Pat(pattern))) {
      mapOfConst(Pat(pattern))
    } else Pat(pattern)

  }

  type OptimizeRule[T] = PartialFunction[Pat[E, T], Pat[E, T]]

  private def coupleOfTwoConst[T]: OptimizeRule[T] = {
    case Pat(x @ CouplePattern(Pat(left @ ConstPattern(a)), Pat(right @ ConstPattern(b)))) =>
      Pat(ConstPattern[E, T](x.func.apply(a, b)))
  }

  private def mapOfConst[T]: OptimizeRule[T] = {
    case Pat(map @ MapPattern(Pat(ConstPattern(x)))) => Pat(ConstPattern[E, T](x.flatMap(map.func)))
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
