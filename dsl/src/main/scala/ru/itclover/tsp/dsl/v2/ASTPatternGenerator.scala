package ru.itclover.tsp.dsl.v2

import cats.{Foldable, Functor, Monad, Order}
import ru.itclover.tsp.core.Window
import ru.itclover.tsp.core.Intervals.{NumericInterval, TimeInterval}
import ru.itclover.tsp.dsl.PatternMetadata
import ru.itclover.tsp.io.{AnyDecodersInstances, Extractor, TimeExtractor}
import ru.itclover.tsp.v2.Pattern.{Idx, IdxExtractor}
import ru.itclover.tsp.v2._
import ru.itclover.tsp.v2.aggregators.{WindowStatistic, WindowStatisticResult}
import scala.reflect.ClassTag
//import ru.itclover.tsp.dsl.v2.FunctionRegistry
import ru.itclover.tsp.v2.aggregators.{AccumPattern, GroupPattern, TimerPattern}
import ru.itclover.tsp.io.AnyDecodersInstances._

import scala.language.implicitConversions
import scala.language.higherKinds
import cats.instances.double._

case class ASTPatternGenerator[Event, EKey, EItem, F[_]: Monad, Cont[_]: Functor: Foldable]()(
  implicit idxExtractor: IdxExtractor[Event],
  timeExtractor: TimeExtractor[Event],
  extractor: Extractor[Event, EKey, EItem],
  @transient fieldToEKey: Symbol => EKey,
  idxOrd: Order[Idx]
) {

  val registry: FunctionRegistry = DefaultFunctionRegistry
  @transient val richPatterns = new Patterns[Event, F, Cont] {}

  trait AnyState[T] extends PState[T, AnyState[T]]

  implicit def toAnyStatePattern[T](p: Pattern[Event, _, _, F, Cont]): Pattern[Event, T, AnyState[T], F, Cont] =
    p.asInstanceOf[Pattern[Event, T, AnyState[T], F, Cont]]

  def build(
    sourceCode: String,
    toleranceFraction: Double,
    fieldsTags: Map[Symbol, ClassTag[_]]
  ): Either[Throwable, (Pattern[Event, Any, AnyState[Any], F, Cont], PatternMetadata)] = {
    val ast = new ASTBuilder(sourceCode, toleranceFraction, fieldsTags).start.run()
    ast.toEither.map(a => (generatePattern(a), a.metadata))
  }

  def generatePattern(ast: AST): Pattern[Event, Any, AnyState[Any], F, Cont] = {
    ast match {
      case c: Constant[_] => ConstPattern[Event, Any, F, Cont](c.value)
      case id: Identifier =>
        id.valueType match {
          case IntASTType =>
            new ExtractingPattern[Event, EKey, EItem, Int, AnyState[Int], F, Cont](id.value, id.value)
          case LongASTType =>
            new ExtractingPattern[Event, EKey, EItem, Long, AnyState[Long], F, Cont](id.value, id.value)
          case DoubleASTType =>
            new ExtractingPattern[Event, EKey, EItem, Double, AnyState[Double], F, Cont](id.value, id.value)
          case BooleanASTType =>
            new ExtractingPattern[Event, EKey, EItem, Boolean, AnyState[Boolean], F, Cont](id.value, id.value)
          case AnyASTType => new ExtractingPattern[Event, EKey, EItem, Any, AnyState[Any], F, Cont](id.value, id.value)
        }
      case r: Range[_] => sys.error(s"Range ($r) is valid only in context of a pattern")
      case fc: FunctionCall[_] =>
        fc.arguments.length match {
          case 1 =>
            val p1 = generatePattern(fc.arguments.head)
            new MapPattern(p1)(
              (x: Any) =>
                Result.succ(
                  registry.functions
                    .getOrElse(
                      (fc.functionName, fc.arguments.map(_.valueType)),
                      sys.error(s"Function ${fc.functionName} not found")
                    )
                    ._1(Seq(x))
              )
            )
          case 2 =>
            val (p1, p2) = (generatePattern(fc.arguments(0)), generatePattern(fc.arguments(1)))
            new CouplePattern(p1, p2)(
              { (x, y) =>
                Result.succ(
                  registry.functions
                    .getOrElse(
                      (fc.functionName, fc.arguments.map(_.valueType)),
                      sys.error(s"Function ${fc.functionName} not found")
                    )
                    ._1(
                      Seq(x.getOrElse(Double.NaN), y.getOrElse(Double.NaN))
                    )
                )
              }
            )
          case _ => sys.error("Functions with 3 or more arguments not yet supported")
        }
      case ffc: ReducerFunctionCall[_] =>
        val (func, _, trans, initial) =
          registry.reducers
            .getOrElse((ffc.functionName, ffc.valueType),
              sys.error(s"Reducer function ${ffc.functionName} with argument type ${ffc.valueType} not found"))
        val wrappedFunc = (x: Result[Any], y: Result[Any]) =>
          (x, y) match {
            case (Fail, _)          => Result.fail
            case (_, Fail)          => Result.fail
            case (Succ(t), Succ(u)) => Result.succ(trans(func(t, u)))
        }
        new ReducePattern(ffc.arguments.map(generatePattern))(wrappedFunc, ffc.cond, Result.succ(initial))
      case ac: AggregateCall[_] =>
        ac.function match {
          case Count =>
            richPatterns.count(
              generatePattern(ac.value)
                .asInstanceOf[Pattern[Event, Double, AnyState[Double], F, Cont]],
              ac.window
            )
          case Sum =>
            richPatterns.sum(
              generatePattern(ac.value)
                .asInstanceOf[Pattern[Event, Double, AnyState[Double], F, Cont]],
              ac.window
            )
          case Avg =>
            richPatterns.avg(
              generatePattern(ac.value)
                .asInstanceOf[Pattern[Event, Double, AnyState[Double], F, Cont]],
              ac.window
            )
        }
      case at: AndThen =>
        AndThenPattern(generatePattern(at.first), generatePattern(at.second))
      // TODO: Window -> TimeInterval in TimerPattern
      case t: Timer             => TimerPattern(generatePattern(t.cond), Window(t.interval.max))
      case fwi: ForWithInterval => new MapPattern(WindowStatistic(generatePattern(fwi.inner), fwi.window))({
        stats: WindowStatisticResult =>
          fwi.interval match {
            case ti: TimeInterval          if ti.contains(stats.successMillis) => Result.succ(true)
            case ni: NumericInterval[Long] if ni.contains(stats.successCount)  => Result.succ(true)
            case _ => Result.fail
          }
      })
      case a: Assert =>
        // TODO@trolley check types
        new MapPattern(generatePattern(a.cond)) ({
          innerBool =>
            if (innerBool.asInstanceOf[Boolean]) Result.succ(()) else Result.fail
        })
    }
  }
}
