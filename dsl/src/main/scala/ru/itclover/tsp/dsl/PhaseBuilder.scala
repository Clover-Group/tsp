package ru.itclover.tsp.dsl

import org.parboiled2.{ErrorFormatter, ParseError}
import ru.itclover.tsp.aggregators.AggregatorPhases.{Aligned, ToSegments}
import ru.itclover.tsp.aggregators.accums.{AccumPhase, PushDownAccumInterval}
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.core.{Pattern, Window}
import ru.itclover.tsp.phases.BooleanPhases.{Assert, BooleanPhaseParser, ComparingParser}
import ru.itclover.tsp.phases.CombiningPhases.{AndThenParser, EitherParser, TogetherParser}
import ru.itclover.tsp.phases.ConstantPhases.OneRowPattern
import ru.itclover.tsp.phases.MonadPhases.{FlatMapParser, MapParser}
import ru.itclover.tsp.phases.NumericPhases.{BinaryNumericParser, Reduce, SymbolNumberExtractor}
import ru.itclover.tsp.phases.TimePhases.Timed
import ru.itclover.tsp.utils.CollectionsOps.TryOps
import ru.itclover.tsp.utils.CollectionsOps.RightBiasedEither

object PhaseBuilder {

  def build[Event](input: String, formatter: ErrorFormatter = new ErrorFormatter())(
    implicit timeExtractor: TimeExtractor[Event],
    symbolNumberExtractor: SymbolNumberExtractor[Event]
  ): Either[String, (Pattern[Event, _, _], PhaseMetadata)] = {
    val parser = new SyntaxParser[Event](input)
    val rawPhase = parser.start.run()
    rawPhase
      .map { p =>
        val maxWindowMs = maxPhaseWindowMs(p)
        (postProcess(p, maxWindowMs), PhaseMetadata(findFields(p).toSet, maxWindowMs))
      }
      .toEither
      .transformLeft {
        case ex: ParseError => formatter.format(ex, input)
        case ex             => throw ex // Unknown exceptional case
      }
  }

  def postProcess[Event](parser: Pattern[Event, _, _], maxPhase: Long, asContinuous: Boolean = false)(
    implicit timeExtractor: TimeExtractor[Event],
    symbolNumberExtractor: SymbolNumberExtractor[Event]
  ): Pattern[Event, _, _] = {
    parser match {
      case ep: EitherParser[Event, _, _, _, _] =>
        EitherParser(postProcess(ep.leftParser, maxPhase), postProcess(ep.rightParser, maxPhase))
      case atp: AndThenParser[Event, _, _, _, _] =>
        AndThenParser(postProcess(atp.first, maxPhase), postProcess(atp.second, maxPhase))
      case tp: TogetherParser[Event, _, _, _, _] =>
        TogetherParser(postProcess(tp.leftParser, maxPhase), postProcess(tp.rightParser, maxPhase))
      case cp: ComparingParser[Event, _, _, _] =>
        new ComparingParser[Event, cp.State1, cp.State2, cp.ExpressionType](
          postProcess(cp.leftParser, maxPhase).asInstanceOf[Pattern[Event, cp.State1, cp.ExpressionType]],
          postProcess(cp.rightParser, maxPhase).asInstanceOf[Pattern[Event, cp.State2, cp.ExpressionType]]
        )(cp.comparingFunction, cp.comparingFunctionName) {}
      case bnp: BinaryNumericParser[Event, _, _, Double] @unchecked =>
        BinaryNumericParser[Event, bnp.State1, bnp.State2, Double](
          postProcess(bnp.left, maxPhase).asInstanceOf[Pattern[Event, bnp.State1, Double]],
          postProcess(bnp.right, maxPhase).asInstanceOf[Pattern[Event, bnp.State2, Double]],
          bnp.operation,
          bnp.operationSign
        )
      case ts: ToSegments[Event, _, _] => ToSegments(postProcess(ts.innerPhase, maxPhase))
      case mp: MapParser[Event, _, _, _] =>
        MapParser[Event, Any, mp.InType, mp.OutType](
          postProcess(mp.phaseParser, maxPhase).asInstanceOf[Pattern[Event, Any, mp.InType]]
        )(mp.function.asInstanceOf[mp.InType => mp.OutType])
      case a: Assert[Event, _] => Assert(postProcess(a.predicate, maxPhase).asInstanceOf[BooleanPhaseParser[Event, _]])
      case aph: AccumPhase[Event, _, _, _] =>
        val withProcessedInners = new AccumPhase[Event, aph.Inner, aph.AccumOutput, aph.Output](
          postProcess(aph.innerPhase, maxPhase, asContinuous = true)
            .asInstanceOf[Pattern[Event, aph.Inner, aph.AccumOutput]],
          aph.window,
          aph.accumulator
        )(aph.extractResult, aph.extractorName) {
          override def toContinuous: AccumPhase[Event, aph.Inner, aph.AccumOutput, aph.Output] = aph.toContinuous
        }
        val processedPhase = if (asContinuous) {
          withProcessedInners.toContinuous
        } else {
          withProcessedInners
        }
        val diff = maxPhase - processedPhase.window.toMillis
        if (diff > 0) Aligned(Window(diff), processedPhase) else processedPhase
      case _ => parser
    }
  }

  def maxPhaseWindowMs[Event](x: Pattern[Event, _, _]): Long = {
    x match {
      case ep: EitherParser[Event, _, _, _, _] =>
        Math.max(maxPhaseWindowMs(ep.leftParser), maxPhaseWindowMs(ep.rightParser))
      case atp: AndThenParser[Event, _, _, _, _] =>
        Math.max(maxPhaseWindowMs(atp.first), maxPhaseWindowMs(atp.second))
      case tp: TogetherParser[Event, _, _, _, _] =>
        Math.max(maxPhaseWindowMs(tp.leftParser), maxPhaseWindowMs(tp.rightParser))
      case cp: ComparingParser[Event, _, _, _] =>
        Math.max(maxPhaseWindowMs(cp.leftParser), maxPhaseWindowMs(cp.rightParser))
      case bnp: BinaryNumericParser[Event, _, _, _] =>
        Math.max(maxPhaseWindowMs(bnp.left), maxPhaseWindowMs(bnp.right))

      case toSegments: ToSegments[Event, _, _]    => maxPhaseWindowMs(toSegments.innerPhase)
      case map: MapParser[Event, _, _, _]         => maxPhaseWindowMs(map.phaseParser)
      case assert: Assert[Event, _]               => maxPhaseWindowMs(assert.predicate)
      case fMap: FlatMapParser[Event, _, _, _, _] => maxPhaseWindowMs(fMap.phase)

      case aph: AccumPhase[Event, _, _, _] =>
        Math.max(aph.window.toMillis, maxPhaseWindowMs(aph.innerPhase))
      case pusher: PushDownAccumInterval[Event, _, _, _] =>
        Math.max(pusher.accum.window.toMillis, maxPhaseWindowMs(pusher.accum.innerPhase))
      case timed: Timed[Event, _, _] =>
        Math.max(timed.timeInterval.min, maxPhaseWindowMs(timed.inner))

      case _ =>
        0L
    }
  }

  def findFields[Event](x: Pattern[Event, _, _]): List[String] = {
    println(x.format(null.asInstanceOf[Event]))
    x match {
      case ep: EitherParser[Event, _, _, _, _]   => findFields(ep.leftParser) ++ findFields(ep.rightParser)
      case atp: AndThenParser[Event, _, _, _, _] => findFields(atp.first) ++ findFields(atp.second)
      case tp: TogetherParser[Event, _, _, _, _] => findFields(tp.leftParser) ++ findFields(tp.rightParser)
      case cp: ComparingParser[Event, _, _, _]   => findFields(cp.leftParser) ++ findFields(cp.rightParser)
      case r: Reduce[Event, _] =>
        findFields(r.firstPhase) ++ r.otherPhases.foldLeft(List[String]())(
          (r: List[String], x: Pattern[Event, _, _]) => r ++ findFields(x)
        )
      case bnp: BinaryNumericParser[Event, _, _, _] => findFields(bnp.left) ++ findFields(bnp.right)
      case aph: AccumPhase[Event, _, _, _]          => findFields(aph.innerPhase)
      case p: PushDownAccumInterval[Event, _, _, _] => findFields(p.accum.innerPhase)
      case ts: ToSegments[Event, _, _]              => findFields(ts.innerPhase)
      case mp: MapParser[Event, _, _, _]            => findFields(mp.phaseParser)
      case a: Assert[Event, _]                      => findFields(a.predicate)
      case t: Timed[Event, _, _]                    => findFields(t.inner)
      case orpp: OneRowPattern[Event, _]            => if (orpp.fieldName.isDefined) List(orpp.fieldName.get.tail) else List()
      case _                                        => List()
    }
  }
}
