package optimizations
import ru.itclover.tsp.v2.Pattern.IdxExtractor
import ru.itclover.tsp.v2.{ConstPattern, CouplePattern, MapPattern, PState, Pattern, Replacable, Result}

trait WithInner[E, S <: PState[T, S], T, P <: Pattern[E, S, T]] extends Replacable[P, P]

class Optimizer {

  def optimize[E, S <: PState[T, S], T](pattern: Pattern[E, S, T]): Pattern[E, S, T] = {
    pattern match {
      case cp: CouplePattern[_, _, _, _, _, _] =>
    }
    ???
  }

  type OptimizeRule[E, T] = PartialFunction[Pattern[E, _, T], (Boolean, Pattern[E, _, T])]

  private def coupleOfTWoConst[E: IdxExtractor, T]: OptimizeRule[E, T] = {
    case cp @ CouplePattern(ConstPattern(l), ConstPattern(r)) =>
      true -> ConstPattern[E, T](cp.func(Result.succ(l), Result.succ(r)))
  }

  private def coupleWithLeftConst[E: IdxExtractor, T]: OptimizeRule[E, T] = {
    case cp @ CouplePattern(ConstPattern(l), r) =>
      true -> new MapPattern(r)(x => cp.func(l, x))
  }


}
