package ru.itclover.tsp.dsl.v2
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Window
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.Pattern.IdxExtractor
import ru.itclover.tsp.v2._
import ru.itclover.tsp.v2.aggregators.{AccumPattern, GroupPattern, TimerPattern}
import scala.language.implicitConversions

class ASTPatternGenerator[Event, F[_]: Monad, Cont[_]: Functor: Foldable](
  implicit idxExtractor: IdxExtractor[Event],
  timeExtractor: TimeExtractor[Event]
) {

  val registry = FunctionRegistry.createDefault

  trait AnyState[T] extends PState[T, AnyState[T]]

  implicit def toAnyStatePattern[T](p: Pattern[Event, _, _, F, Cont]): Pattern[Event, T, AnyState[T], F, Cont] =
    p.asInstanceOf[Pattern[Event, T, AnyState[T], F, Cont]]

  def generatePattern[T](ast: AST[T]): Pattern[Event, T, AnyState[T], F, Cont] = {
    ast match {
      case c: Constant[_] => ConstPattern[Event, T, F, Cont](c.value)
      case id: Identifier => ??? // TODO: Extracting pattern
      case r: Range[_]    => ??? // should throw exception, since a range is not a complete pattern
      case fc: FunctionCall[_] =>
        fc.arguments.length match {
          case 1 =>
            new MapPattern(generatePattern(fc.arguments.head))(
              registry.getFunction1(fc.functionName).get
            )
          case 2 =>
            new CouplePattern(generatePattern(fc.arguments(0)), generatePattern(fc.arguments(1)))(
              registry.getFunction2(fc.functionName).get
            )
          case _ => ??? // TODO: Temporarily throw exception
        }
      // TODO: Function registry for reduce
      case ffc: FilteredFunctionCall[_] => new ReducePattern(ffc.arguments.map(generatePattern))(???, ffc.cond, ???)
      case psc: PatternStatsCall[_]     => psc.functionName match {
        case _ => ???
      }
      // TODO: Function registry for aggregate
      case ac: AggregateCall[_]         => ??? //GroupPattern(generatePattern(ac.value), ac.window) // TODO: Which pattern?
      case at: AndThen =>
        AndThenPattern(generatePattern(at.first), generatePattern(at.second))
      // TODO: Window -> TimeInterval in TimerPattern
      case t: Timer => TimerPattern(generatePattern(t.cond), Window(t.interval.min))
      case f: For   => ???
      case a: Assert =>
        new MapPattern(generatePattern[Boolean](a.cond))(
          innerBool => if (innerBool) Result.succ(()) else Result.fail
        )
    }
  }
}
