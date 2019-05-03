package ru.itclover.tsp.core.optimizations
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core._

import scala.language.reflectiveCalls

trait WithInner[E, S <: PState[T, S], T, P <: Pattern[E, S, T]] extends Replacable[P, P]

class Optimizer[E: IdxExtractor] {

  def optimize[S <: PState[T, S], T](pattern: Pattern[E, S, T]): Pattern[E, S, T] = {

    coupleOfTwoConst(pattern.asInstanceOf[Pat[T]])._2.asInstanceOf[Pattern[E, S, T]]
  }

  type Pat[T] = ({
    type S <: PState[T, S]
    type Inner = Pattern[E, S, T]
  })#Inner

  type OptimizeRule[T] = PartialFunction[Pat[T], (Boolean, Pat[T])]

  private def coupleOfTwoConst[S1, S2, T1, T2, T]: OptimizeRule[T] = {
    case x: CouplePattern[E, S1, S2, T1, T2, T]
        if x.left.isInstanceOf[ConstPattern[_, _]] && x.right.isInstanceOf[ConstPattern[_, _]] =>
      true -> ConstPattern(
        x.func.apply(x.left.asInstanceOf[ConstPattern[E, T1]].value, x.right.asInstanceOf[ConstPattern[E, T2]].value)
      ).asInstanceOf[Pat[T]]
  }
}
