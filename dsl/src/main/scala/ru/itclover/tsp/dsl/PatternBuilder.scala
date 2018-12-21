package ru.itclover.tsp.dsl

import org.parboiled2.{ErrorFormatter, ParseError}
import cats.syntax.either._
import ru.itclover.tsp.aggregators.AggregatorPhases.{Aligned, Skip, SegmentsPattern}
import ru.itclover.tsp.aggregators.accums.{AccumPhase, PushDownAccumInterval}
import ru.itclover.tsp.core.{Pattern, Window}
import ru.itclover.tsp.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.patterns.Booleans.{Assert, BooleanPhaseParser, ComparingPattern}
import ru.itclover.tsp.patterns.Combining.{AndThenParser, EitherParser, TogetherParser}
import ru.itclover.tsp.patterns.Constants.ExtractingPattern
import ru.itclover.tsp.patterns.Monads.{FlatMapParser, MapParser}
import ru.itclover.tsp.patterns.Numerics.{BinaryNumericParser, Reduce}
import ru.itclover.tsp.patterns.TimePhases.Timed
import ru.itclover.tsp.utils.CollectionsOps.TryOps
import ru.itclover.tsp.Segment
import scala.math.Numeric.DoubleIsFractional

object PatternBuilder {

  def build[Event, EKey, EItem](
    input: String,
    idToEKey: Symbol => EKey,
    toleranceFraction: Double,
    formatter: ErrorFormatter = new ErrorFormatter()
  )(
    implicit timeExtractor: TimeExtractor[Event],
    extractor: Extractor[Event, EKey, EItem],
    decodeDouble: Decoder[EItem, Double]
  ): Either[String, (Pattern[Event, _, Segment], PatternMetadata)] = {
    val parser = new SyntaxParser[Event, EKey, EItem](input, idToEKey, toleranceFraction)
    val rawPattern = parser.start.run()
    rawPattern
      .map { p =>
        val maxWindowMs = maxPhaseWindowMs(p)
        val pattern = SegmentsPattern(postProcess(p, maxWindowMs))
        (pattern, PatternMetadata(findFields(p).toSet, maxWindowMs))
      }
      .toEither
      .leftMap {
        case ex: ParseError => formatter.format(ex, input)
        case ex             => throw ex // Unknown exceptional case
      }
  }

  def postProcess[Event](parser: Pattern[Event, _, _], maxPhase: Long, asContinuous: Boolean = false)(
    implicit timeExtractor: TimeExtractor[Event]
  ): Pattern[Event, _, _] = {
    parser match {
      case ep: EitherParser[Event, _, _, _, _] =>
        ep.copy(leftParser = postProcess(ep.leftParser, maxPhase), rightParser = postProcess(ep.rightParser, maxPhase))
      case atp: AndThenParser[Event, _, _, _, _] =>
        atp.copy(first = postProcess(atp.first, maxPhase), second = postProcess(atp.second, maxPhase))
      case tp: TogetherParser[Event, _, _, _, _] =>
        tp.copy(leftParser = postProcess(tp.leftParser, maxPhase), rightParser = postProcess(tp.rightParser, maxPhase))
      case cp: ComparingPattern[Event, _, _, _] =>
        cp.copy(
          left = postProcess(cp.left, maxPhase),
          right = postProcess(cp.right, maxPhase)
        )(cp.comparingFunction.asInstanceOf[(Any, Any) => Boolean], cp.comparingFunctionName)
      case bnp: BinaryNumericParser[Event, _, _, Double] @unchecked =>
        bnp.copy(
          left = postProcess(bnp.left, maxPhase).asInstanceOf[Pattern[Event, _, Double]],
          right = postProcess(bnp.right, maxPhase).asInstanceOf[Pattern[Event, _, Double]]
        )
      case ts: SegmentsPattern[Event, _, _] => ts.copy(inner = postProcess(ts.inner, maxPhase))
      case mp: MapParser[Event, _, _, _] =>
        mp.copy(phaseParser = postProcess(mp.phaseParser, maxPhase))(mp.function.asInstanceOf[Any => _])
      case a: Assert[Event, _] =>
        a.copy(predicate = postProcess(a.predicate, maxPhase).asInstanceOf[BooleanPhaseParser[Event, _]])
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
      case cp: ComparingPattern[Event, _, _, _] =>
        Math.max(maxPhaseWindowMs(cp.left), maxPhaseWindowMs(cp.right))
      case bnp: BinaryNumericParser[Event, _, _, _] =>
        Math.max(maxPhaseWindowMs(bnp.left), maxPhaseWindowMs(bnp.right))

      case toSegments: SegmentsPattern[Event, _, _]    => maxPhaseWindowMs(toSegments.inner)
      case map: MapParser[Event, _, _, _]         => maxPhaseWindowMs(map.phaseParser)
      case assert: Assert[Event, _]               => maxPhaseWindowMs(assert.predicate)
      case fMap: FlatMapParser[Event, _, _, _, _] => maxPhaseWindowMs(fMap.phase)

      case aph: AccumPhase[Event, _, _, _] =>
        Math.max(aph.window.toMillis, maxPhaseWindowMs(aph.innerPhase))
      case pusher: PushDownAccumInterval[Event, _, _, _] =>
        Math.max(pusher.accum.window.toMillis, maxPhaseWindowMs(pusher.accum.innerPhase))
      case timed: Timed[Event, _, _] =>
        Math.max(timed.timeInterval.max, maxPhaseWindowMs(timed.inner))

      case _ =>
        0L
    }
  }

  def findFields[Event](x: Pattern[Event, _, _]): List[String] = {
    //println(x.format(null.asInstanceOf[Event]))
    x match {
      case ep: EitherParser[Event, _, _, _, _]   => findFields(ep.leftParser) ++ findFields(ep.rightParser)
      case atp: AndThenParser[Event, _, _, _, _] => findFields(atp.first) ++ findFields(atp.second)
      case tp: TogetherParser[Event, _, _, _, _] => findFields(tp.leftParser) ++ findFields(tp.rightParser)
      case cp: ComparingPattern[Event, _, _, _]   => findFields(cp.left) ++ findFields(cp.right)
      case r: Reduce[Event, _] =>
        findFields(r.firstPhase) ++ r.otherPhases.foldLeft(List[String]())(
          (r: List[String], x: Pattern[Event, _, _]) => r ++ findFields(x)
        )
      case bnp: BinaryNumericParser[Event, _, _, _] => findFields(bnp.left) ++ findFields(bnp.right)
      case aph: AccumPhase[Event, _, _, _]          => findFields(aph.innerPhase)
      case p: PushDownAccumInterval[Event, _, _, _] => findFields(p.accum.innerPhase)
      case ts: SegmentsPattern[Event, _, _]              => findFields(ts.inner)
      case mp: MapParser[Event, _, _, _]            => findFields(mp.phaseParser)
      case fmp: FlatMapParser[Event, _, _, _, _]    => findFields(fmp.phase)
      case s: Skip[Event, _, _]                     => findFields(s.phase)
      case a: Assert[Event, _]                      => findFields(a.predicate)
      case t: Timed[Event, _, _]                    => findFields(t.inner)
      case e: ExtractingPattern[Event, _, _, _]     => List(e.keyName.toString.tail)
      case _                                        => List()
    }
  }
}
