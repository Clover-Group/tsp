package ru.itclover.streammachine.newsyntax

import org.parboiled2.{ErrorFormatter, ParseError}
import ru.itclover.streammachine.aggregators.AggregatorPhases.{Aligned, ToSegments}
import ru.itclover.streammachine.aggregators.accums.AccumPhase
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.core.{PhaseParser, Window}
import ru.itclover.streammachine.phases.BooleanPhases.{Assert, BooleanPhaseParser, ComparingParser}
import ru.itclover.streammachine.phases.CombiningPhases.{AndThenParser, EitherParser, TogetherParser}
import ru.itclover.streammachine.phases.ConstantPhases.OneRowPhaseParser
import ru.itclover.streammachine.phases.MonadPhases.MapParser
import ru.itclover.streammachine.phases.NumericPhases.{BinaryNumericParser, SymbolNumberExtractor}
import ru.itclover.streammachine.utils.CollectionsOps.TryOps
import ru.itclover.streammachine.utils.CollectionsOps.RightBiasedEither


object PhaseBuilder {
  def build[Event](input: String, formatter: ErrorFormatter = new ErrorFormatter())
                  (implicit timeExtractor: TimeExtractor[Event],
                   symbolNumberExtractor: SymbolNumberExtractor[Event]):
  Either[String, (PhaseParser[Event, _, _], List[String])] = {
    val parser = new SyntaxParser[Event](input)
    val rawPhase = parser.start.run()
    rawPhase.map(p => (postProcess(p, maxTimePhase(p)), fields(p))).toEither.transformLeft {
      case ex: ParseError => formatter.format(ex, input)
      case ex => throw ex // Unknown exceptional case
    }
  }


  def postProcess[Event](x: PhaseParser[Event, _, _], maxPhase: Long, asContinuous: Boolean = false)
                        (implicit timeExtractor: TimeExtractor[Event],
                         symbolNumberExtractor: SymbolNumberExtractor[Event]): PhaseParser[Event, _, _] = {
    //println(x.formatWithInitialState(null.asInstanceOf[Event]), asContinuous)
    x match {
      case ep: EitherParser[Event, _, _, _, _] => EitherParser(postProcess(ep.leftParser, maxPhase),
        postProcess(ep.rightParser, maxPhase))
      case atp: AndThenParser[Event, _, _, _, _] => AndThenParser(postProcess(atp.first, maxPhase),
        postProcess(atp.second, maxPhase))
      case tp: TogetherParser[Event, _, _, _, _] => TogetherParser(postProcess(tp.leftParser, maxPhase),
        postProcess(tp.rightParser, maxPhase))
      case cp: ComparingParser[Event, _, _, _] => new ComparingParser[Event, cp.State1, cp.State2, cp.ExpressionType](
        postProcess(cp.leftParser, maxPhase).asInstanceOf[PhaseParser[Event, cp.State1, cp.ExpressionType]],
        postProcess(cp.rightParser, maxPhase).asInstanceOf[PhaseParser[Event, cp.State2, cp.ExpressionType]]
      )(cp.comparingFunction, cp.comparingFunctionName) {
      }
      // TODO Fix warning
      case bnp: BinaryNumericParser[Event, _, _, Double] => BinaryNumericParser[Event, bnp.State1, bnp.State2, Double](
        postProcess(bnp.left, maxPhase).asInstanceOf[PhaseParser[Event, bnp.State1, Double]],
        postProcess(bnp.right, maxPhase).asInstanceOf[PhaseParser[Event, bnp.State2, Double]],
        bnp.operation, bnp.operationSign)
      case ts: ToSegments[Event, _, _] => ToSegments(postProcess(ts.innerPhase, maxPhase))
      case mp: MapParser[Event, _, _, _] => MapParser[Event, Any, mp.InType, mp.OutType](
        postProcess(mp.phaseParser, maxPhase).asInstanceOf[PhaseParser[Event, Any, mp.InType]])(
        mp.function.asInstanceOf[mp.InType => mp.OutType])
      case a: Assert[Event, _] => Assert(postProcess(a.predicate, maxPhase).asInstanceOf[BooleanPhaseParser[Event, _]])
      case aph: AccumPhase[Event, _, _, _] =>
        val q = new AccumPhase[Event, aph.Inner, aph.AccumOutput, aph.Output](
          postProcess(aph.innerPhase, maxPhase,
            asContinuous = true).asInstanceOf[PhaseParser[Event, aph.Inner, aph.AccumOutput]],
          aph.window, aph.accumulator)(aph.extractResult, aph.extractorName) {
          override def toContinuous: AccumPhase[Event, aph.Inner, aph.AccumOutput, aph.Output] = aph.toContinuous
        }
        val a = if (asContinuous) {
          q.toContinuous
        } else {
          q
        }
        val diff = maxPhase - a.window.toMillis
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
      case aph: AccumPhase[Event, _, _, _] => aph.window.toMillis
      case ts: ToSegments[Event, _, _] => maxTimePhase(ts.innerPhase)
      case mp: MapParser[Event, _, _, _] => maxTimePhase(mp.phaseParser)
      case a: Assert[Event, _] => maxTimePhase(a.predicate)
      case _ => 0L
    }
  }

  def fields[Event](x: PhaseParser[Event, _, _]): List[String] = {
    x match {
      case ep: EitherParser[Event, _, _, _, _] => fields(ep.leftParser) ++ fields(ep.rightParser)
      case atp: AndThenParser[Event, _, _, _, _] => fields(atp.first) ++ fields(atp.second)
      case tp: TogetherParser[Event, _, _, _, _] => fields(tp.leftParser) ++ fields(tp.rightParser)
      case cp: ComparingParser[Event, _, _, _] => fields(cp.leftParser) ++ fields(cp.rightParser)
      case bnp: BinaryNumericParser[Event, _, _, _] => fields(bnp.left) ++ fields(bnp.right)
      case aph: AccumPhase[Event, _, _, _] => fields(aph.innerPhase)
      case ts: ToSegments[Event, _, _] => fields(ts.innerPhase)
      case mp: MapParser[Event, _, _, _] => fields(mp.phaseParser)
      case a: Assert[Event, _] => fields(a.predicate)
      case orpp: OneRowPhaseParser[Event, _] => if (orpp.fieldName.isDefined) List(orpp.fieldName.get) else List()
      case _ => List()
    }
  }
}