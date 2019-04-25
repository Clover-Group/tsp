//package optimizations
//import ru.itclover.tsp.v2.{ConstPattern, CouplePattern, MapPattern, PState, Pattern, Replacable}
//
//trait WithInner[E, S <: PState[T, S], T, P <: Pattern[E, S, T]] extends Replacable[P, P]
//
//class Optimizer {
//
//  def optimize[E, S <: PState[T, S], T](pattern: Pattern[E, S, T]): Pattern[E, S, T] = {
//    pattern match {
//      case cp: CouplePattern[_, _, _,_, _, _] =>
//
//      case mp: MapPattern[] => ???
//
//
//
//    }
//    ???
//  }
//
//  trait OptimizeRule[E, S,T] extends PartialFunction[Pattern[E, S,T], (Boolean, Pattern[E, S ,T])]
//
//  private def coupleOfTWoConst[E, S,T]: OptimizeRule[E,S,T] = new OptimizeRule[E,S,T] {
//    override def isDefinedAt(x: Pattern[E,S,T]): Boolean = false
//
//    override def apply(v1: Pattern[E,S,T]): (Boolean, Pattern[E,S,T]) = v1 match {
//      case c: CouplePattern if c.left.isInstanceOf[ConstPattern] && c.right.isInstanceOf[ConstPattern] =>
//    }
//  }
//}
