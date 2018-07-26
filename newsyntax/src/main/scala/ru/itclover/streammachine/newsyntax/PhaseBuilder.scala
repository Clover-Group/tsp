package ru.itclover.streammachine.newsyntax

import ru.itclover.streammachine.aggregators.AggregatorPhases.{Aligned, ToSegments}
import ru.itclover.streammachine.aggregators.accums.AccumPhase
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.core.{PhaseParser, Window}
import ru.itclover.streammachine.phases.BooleanPhases.{Assert, BooleanPhaseParser, ComparingParser}
import ru.itclover.streammachine.phases.CombiningPhases.{AndThenParser, EitherParser, TogetherParser}
import ru.itclover.streammachine.phases.MonadPhases.MapParser
import ru.itclover.streammachine.phases.NumericPhases.{BinaryNumericParser, SymbolNumberExtractor}

import scala.util.{Failure, Success, Try}

object PhaseBuilder {
  def build[Event](x: String)(implicit timeExtractor: TimeExtractor[Event],
                              symbolNumberExtractor: SymbolNumberExtractor[Event]):
  (Try[PhaseParser[Event, _, _]], SyntaxParser[Event]) = {
    val parser = new SyntaxParser[Event](x)
    val prep = parser.start.run()
    (if (prep.isFailure) {
      prep
    } else {
      try {
        Success(postProcess(prep.get, maxTimePhase(prep.get)))
      } catch {
        case e: Throwable => Failure(e)
      }
    }, parser)
  }


  def postProcess[Event](x: PhaseParser[Event, _, _], mtf: Long, asContinuous: Boolean = false)
                        (implicit timeExtractor: TimeExtractor[Event],
                         symbolNumberExtractor: SymbolNumberExtractor[Event]): PhaseParser[Event, _, _] = {
    //println(x.formatWithInitialState(null.asInstanceOf[Event]), asContinuous)
    x match {
      case ep: EitherParser[Event, _, _, _, _] => EitherParser(postProcess(ep.leftParser, mtf),
        postProcess(ep.rightParser, mtf))
      case atp: AndThenParser[Event, _, _, _, _] => AndThenParser(postProcess(atp.first, mtf),
        postProcess(atp.second, mtf))
      case tp: TogetherParser[Event, _, _, _, _] => TogetherParser(postProcess(tp.leftParser, mtf),
        postProcess(tp.rightParser, mtf))
      case cp: ComparingParser[Event, _, _, _] => new ComparingParser[Event, cp.State1, cp.State2, cp.ExpressionType](
        postProcess(cp.leftParser, mtf).asInstanceOf[PhaseParser[Event, cp.State1, cp.ExpressionType]],
        postProcess(cp.rightParser, mtf).asInstanceOf[PhaseParser[Event, cp.State2, cp.ExpressionType]]
      )(cp.comparingFunction, cp.comparingFunctionName) {
      }
      case bnp: BinaryNumericParser[Event, _, _, Double] => BinaryNumericParser[Event, bnp.State1, bnp.State2, Double](
        postProcess(bnp.left, mtf).asInstanceOf[PhaseParser[Event, bnp.State1, Double]],
        postProcess(bnp.right, mtf).asInstanceOf[PhaseParser[Event, bnp.State2, Double]],
        bnp.operation, bnp.operationSign)
      case ts: ToSegments[Event, _, _] => ToSegments(postProcess(ts.innerPhase, mtf))
      case mp: MapParser[Event, _, _, _] => MapParser[Event, Any, mp.InType, mp.OutType](
        postProcess(mp.phaseParser, mtf).asInstanceOf[PhaseParser[Event, Any, mp.InType]])(
        mp.function.asInstanceOf[mp.InType => mp.OutType])
      case a: Assert[Event, _] => Assert(postProcess(a.predicate, mtf).asInstanceOf[BooleanPhaseParser[Event, _]])
      case aph: AccumPhase[Event, _, _, _] =>
        val q = new AccumPhase[Event, aph.Inner, aph.AccumOutput, aph.Output](
          postProcess(aph.inner, mtf, asContinuous = true).asInstanceOf[PhaseParser[Event, aph.Inner, aph.AccumOutput]],
          aph.timeWindow, aph.accum)(aph.extractor, aph.exName) {
          override def toContinuous: AccumPhase[Event, aph.Inner, aph.AccumOutput, aph.Output] = aph.toContinuous
        }
        val a = if (asContinuous) {
          q.toContinuous
        } else {
          q
        }
        val diff = mtf - a.timeWindow.toMillis
        if (diff > 0) Aligned(Window(diff), a) else a
      case _ => x
    }
  }

  def maxTimePhase[Event](x: PhaseParser[Event, _, _]): Long = {
    x match {
      case ep: EitherParser[Event, _, _, _, _] => Math.max(maxTimePhase(ep.leftParser), maxTimePhase(ep.rightParser))
      case atp: AndThenParser[Event, _, _, _, _] => Math.max(maxTimePhase(atp.first), maxTimePhase(atp.second))
      case tp: TogetherParser[Event, _, _, _, _] => Math.max(maxTimePhase(tp.leftParser), maxTimePhase(tp.rightParser))
      case cp: ComparingParser[Event, _, _, _] => Math.max(maxTimePhase(cp.leftParser), maxTimePhase(cp.rightParser))
      case bnp: BinaryNumericParser[Event, _, _, _] => Math.max(maxTimePhase(bnp.left), maxTimePhase(bnp.right))
      case aph: AccumPhase[Event, _, _, _] => aph.timeWindow.toMillis
      case ts: ToSegments[Event, _, _] => maxTimePhase(ts.innerPhase)
      case mp: MapParser[Event, _, _, _] => maxTimePhase(mp.phaseParser)
      case a: Assert[Event, _] => maxTimePhase(a.predicate)
      case _ => 0L
    }
  }
}