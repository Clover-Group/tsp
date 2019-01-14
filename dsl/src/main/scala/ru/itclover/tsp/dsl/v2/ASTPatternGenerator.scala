package ru.itclover.tsp.dsl.v2
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.v2.Pattern.IdxExtractor
import ru.itclover.tsp.v2._

class ASTPatternGenerator[Event, F[_]: Monad, Cont[_]: Functor: Foldable](
  implicit idxExtractor: IdxExtractor[Event],
  timeExtractor: TimeExtractor[Event]
) {

  val registry = FunctionRegistry.createDefault

  trait AnyState[T] extends PState[T, AnyState[T]]

  implicit def toAny[T](p: Pattern[Event, _, _, F, Cont]): Pattern[Event, T, AnyState[T], F, Cont] =
    p.asInstanceOf[Pattern[Event, T, AnyState[T], F, Cont]]

  def generatePattern[T](ast: AST[T]): Pattern[Event, T, AnyState[T], F, Cont] = {
    ast match {
      case c: Constant[_]               => ConstPattern[Event, T, F, Cont](c.value)
      case id: Identifier               => ???
      case r: Range[_]                  => ???
      case fc: FunctionCall[_]          => fc.arguments.length match {
        case 1 => new MapPattern(generatePattern(fc.arguments.head))(???)
        case 2 => new CouplePattern(generatePattern(fc.arguments(0)), generatePattern(fc.arguments(1)))(???)
        case _ => ???
      }
      case ffc: FilteredFunctionCall[_] => ???
      case psc: PatternStatsCall[_]     => ???
      case ac: AggregateCall[_]         => ???
      case at: AndThen =>
        AndThenPattern(generatePattern(at.first), generatePattern(at.second))
      case t: Timer => ???
      case f: For   => ???
      case a: Assert =>
        new MapPattern(generatePattern[Boolean](a.cond))(
          innerBool => if (innerBool) Result.succ(()) else Result.fail
        )
    }
  }
}
