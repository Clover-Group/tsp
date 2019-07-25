package ru.itclover.tsp.core.optimizations
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.Pat
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.Pattern.WithInners
import scala.language.existentials

import scala.language.reflectiveCalls

class Optimizer[E: IdxExtractor] {

  def optimizations = Seq(coupleOfTwoSimple, coupleOfTwoConst, mapOfConst, mapOfSimple)

  def optimizable(pat: Pat[E]): Boolean =
    optimizations.foldLeft(false) {
      case (collect, rule) => collect || rule.isDefinedAt(pat)
    }

  def applyOptimizations(pat: Pat[E]): (Pat[E], Boolean) = {
    optimizations.foldLeft(pat -> false) {
      case ((p, _), rule) if rule.isDefinedAt(p) => rule.apply(p).asInstanceOf[Pat[E]] -> true
      case (x, _)                                => x
    }
  }

  def optimize(pattern: Pat[E]): Pat[E] = {

    val optimizedPatternIterator = Iterator.iterate(pattern -> true) { case (p, _) => applyOptimizations(p) }

    // try no more than 10 cycles of optimizations to avoid infinite recursive loops in case of wrong rule.
    optimizedPatternIterator.takeWhile(_._2).take(10).map(_._1).toSeq.last
  }

  type OptimizeRule = PartialFunction[Pat[E], Pat[E]]

  private def coupleOfTwoConst[T]: OptimizeRule = {
    case Pat(x @ CouplePattern(Pat(ConstPattern(a)), Pat(ConstPattern(b)))) =>
      ConstPattern[E, T](x.func.apply(a, b))
  }

  private def coupleOfTwoSimple[T]: OptimizeRule = {
    case Pat(x @ CouplePattern(Pat(left @ SimplePattern(fleft)), Pat(right @ SimplePattern(fright)))) =>
      SimplePattern[E, T](
        event => x.func.apply(fleft.apply(event.asInstanceOf[Nothing]), fright.apply(event.asInstanceOf[Nothing]))
      )
  }

  private def mapOfConst[T]: OptimizeRule = {
    case Pat(map @ MapPattern(Pat(ConstPattern(x)))) => ConstPattern[E, T](x.flatMap(map.func))
  }

  private def mapOfSimple[T]: OptimizeRule = {
    case Pat(map @ MapPattern(Pat(simple: SimplePattern[E, _]))) =>
      SimplePattern[E, T](simple.f.andThen(map.func))
  }

  private def optimizeInners[T]: OptimizeRule[T] = {
  case x : WithInners[E] if x.innerPatterns.any(optimizable) => 
  }

}
