package ru.itclover.tsp.dsl.v2

import cats.Order
import ru.itclover.tsp.core.Intervals.{NumericInterval, TimeInterval}
import ru.itclover.tsp.core.Window
import ru.itclover.tsp.dsl.PatternMetadata
import ru.itclover.tsp.io.{Extractor, TimeExtractor}
import ru.itclover.tsp.v2.Pattern.{Idx, IdxExtractor}
import ru.itclover.tsp.v2._
import ru.itclover.tsp.v2.aggregators.{WindowStatistic, WindowStatisticResult}

import scala.reflect.ClassTag
import cats.instances.double._
import ru.itclover.tsp.io.AnyDecodersInstances._
import ru.itclover.tsp.v2.aggregators.TimerPattern

import scala.language.{higherKinds, implicitConversions}
import com.typesafe.scalalogging.Logger


case class ASTPatternGenerator[Event, EKey, EItem]()(
  implicit idxExtractor: IdxExtractor[Event],
  timeExtractor: TimeExtractor[Event],
  extractor: Extractor[Event, EKey, EItem],
  @transient fieldToEKey: Symbol => EKey,
  idxOrd: Order[Idx]
) {

  val registry: FunctionRegistry = DefaultFunctionRegistry
  @transient val richPatterns = new Patterns[Event] {}

  private val log = Logger("ASTPGenLogger")

  trait AnyState[T] extends PState[T, AnyState[T]]

  implicit def toAnyStatePattern[T](p: Pattern[Event, _, _]): Pattern[Event, AnyState[T], T] =
    p.asInstanceOf[Pattern[Event, AnyState[T], T]]

  implicit def toResult[T](value: T): Result[T] = Result.succ(value)

  def build(
    sourceCode: String,
    toleranceFraction: Double,
    fieldsTags: Map[Symbol, ClassTag[_]]
  ): Either[Throwable, (Pattern[Event, AnyState[Any], Any], PatternMetadata)] = {
    val ast = new ASTBuilder(sourceCode, toleranceFraction, fieldsTags).start.run()
    ast.toEither.map(a => (generatePattern(a), a.metadata))
  
  }

  def generatePattern(ast: AST): Pattern[Event, AnyState[Any], Any] = {

    ast match {
      case c: Constant[_] => ConstPattern[Event, Any](c.value)
      case id: Identifier =>
        id.valueType match {
          case IntASTType =>
            new ExtractingPattern[Event, EKey, EItem, Int, AnyState[Int]](id.value, id.value)
          case LongASTType =>
            new ExtractingPattern[Event, EKey, EItem, Long, AnyState[Long]](id.value, id.value)
          case DoubleASTType =>
            new ExtractingPattern[Event, EKey, EItem, Double, AnyState[Double]](id.value, id.value)
          case BooleanASTType =>
            new ExtractingPattern[Event, EKey, EItem, Boolean, AnyState[Boolean]](id.value, id.value)
          case AnyASTType => new ExtractingPattern[Event, EKey, EItem, Any, AnyState[Any]](id.value, id.value)
        }
      case r: Range[_] => sys.error(s"Range ($r) is valid only in context of a pattern")
      case fc: FunctionCall =>
        fc.arguments.length match {
          case 1 =>
            val p1 = generatePattern(fc.arguments(0))
            new MapPattern(p1)(
              (x: Any) =>
                  registry.functions
                    .getOrElse(
                      (fc.functionName, fc.arguments.map(_.valueType)),
                      sys.error(
                        s"Function ${fc.functionName} with argument types " +
                        s"(${fc.arguments.map(_.valueType).mkString(",")})  not found"
                      )
                    )
                    ._1(Seq(x))
            )
          case 2 =>

            log.debug(s"Case 2 called: Arg0 = $fc.arguments(0), Arg1 = $fc.arguments(1)")
            val (p1, p2) = (generatePattern(fc.arguments(0)), generatePattern(fc.arguments(1)))
            new CouplePattern(p1, p2)(
              { (x, y) =>
                (x, y) match {
                  case (Succ(rx), Succ(ry)) =>
                      registry.functions
                        .getOrElse(
                          (fc.functionName, fc.arguments.map(_.valueType)),
                          sys.error(
                            s"Function ${fc.functionName} with argument types " +
                            s"(${fc.arguments.map(_.valueType).mkString(",")}) not found"
                          )
                        )
                        ._1(
                          Seq(rx,ry) // <--- TSP-182 fails here
                        )

                  case _ => Result.fail
                }
              }
            )
          case _ => sys.error("Functions with 3 or more arguments not yet supported")
        }
      case ffc: ReducerFunctionCall =>
        val (func, _, trans, initial) =
          registry.reducers
            .getOrElse(
              (ffc.functionName, ffc.valueType),
              sys.error(s"Reducer function ${ffc.functionName} with argument type ${ffc.valueType} not found")
            )
        val wrappedFunc = (x: Result[Any], y: Result[Any]) =>
          (x, y) match {
            case (Fail, _)          => Result.fail
            case (_, Fail)          => Result.fail
            case (Succ(t), Succ(u)) => Result.succ(trans(func(t, u)))
        }
        new ReducePattern(ffc.arguments.map(generatePattern))(wrappedFunc, ffc.cond, Result.succ(initial))

      // case AggregateCall(Count, inner, w) if inner.valueType == DoubleASTType => ??? // this way

      case ac: AggregateCall =>
        ac.function match {
          case Count =>
            richPatterns.count(
              generatePattern(ac.value)
                .asInstanceOf[Pattern[Event, AnyState[Double], Double]],
              ac.window
            )
          case Sum =>
            richPatterns.sum(
              generatePattern(ac.value)
                .asInstanceOf[Pattern[Event, AnyState[Double], Double]],
              ac.window
            )
          case Avg =>
            richPatterns.avg(
              generatePattern(ac.value)
                .asInstanceOf[Pattern[Event, AnyState[Double], Double]],
              ac.window
            )
          case Lag =>
            richPatterns.lag(
              generatePattern(ac.value)
                .asInstanceOf[Pattern[Event, AnyState[Double], Double]],
              ac.window
            )
        }
      case at: AndThen =>
        // Pair of indices indicates success, so we convert it to true
        new MapPattern(AndThenPattern(generatePattern(at.first), generatePattern(at.second)))(
          v => if (v.isInstanceOf[(Idx, Idx)]) true else v
        )
      // TODO: Window -> TimeInterval in TimerPattern
      case t: Timer => TimerPattern(generatePattern(t.cond), Window(t.interval.max))
      case fwi: ForWithInterval =>
        new MapPattern(WindowStatistic(generatePattern(fwi.inner), fwi.window))({ stats: WindowStatisticResult =>
          // should wait till the end of the window?
          val exactly = fwi.exactly.getOrElse(false) || (
            fwi.interval match {
              case TimeInterval(_, max)    => max < fwi.window.toMillis
              case NumericInterval(_, end) => end.getOrElse(Long.MaxValue) < Long.MaxValue
              case _                       => true
            })
          val isWindowEnded = !exactly || stats.totalMillis >= fwi.window.toMillis
          fwi.interval match {
            case ti: TimeInterval if ti.contains(stats.successMillis) && isWindowEnded         => Result.succ(true)
            case ni: NumericInterval[Long] if ni.contains(stats.successCount) && isWindowEnded => Result.succ(true)
            case _                                                                             => Result.fail
          }
        })

      case c: Cast =>
        c.to match {
          case IntASTType     => new MapPattern(generatePattern(c.inner))(decodeToInt(_))
          case LongASTType    => new MapPattern(generatePattern(c.inner))(decodeToLong(_))
          case BooleanASTType => new MapPattern(generatePattern(c.inner))(decodeToBoolean(_))
          case StringASTType  => new MapPattern(generatePattern(c.inner))(decodeToString(_))
          case DoubleASTType  => new MapPattern(generatePattern(c.inner))(decodeToDouble(_))
          case AnyASTType     => new MapPattern(generatePattern(c.inner))(decodeToAny(_))
        }

      case Assert(inner) if inner.valueType == BooleanASTType =>
        new MapPattern(generatePattern(inner))({ innerBool =>
          if (innerBool.asInstanceOf[Boolean]) Result.succ(innerBool) else Result.fail
        })
      case Assert(inner) if inner.valueType != BooleanASTType =>
        sys.error(s"Invalid pattern, non-boolean pattern inside of Assert - $inner")

      case notImplemented => sys.error(s"AST $notImplemented is not implemented yet.")
    }
  }
}
