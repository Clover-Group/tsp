package optimizations
import ru.itclover.tsp.v2.{CouplePattern, MapPattern, PState, Pattern}

class Optimizer {

  def optimize[E, S <: PState[T, S], T](pattern: Pattern[E, S, T]): Pattern[E, S, T] = {
    pattern match {
      case cp : CouplePattern[]
      case mp:MapPattern[] => ???

    }
    ???
  }



}
