package ru.itclover.streammachine

import cats.data.Validated
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import ru.itclover.streammachine.core.PhaseParser
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.io.input.{InputConf, RawPattern}
import ru.itclover.streammachine.io.output.OutputConf
import ru.itclover.streammachine.newsyntax.PhaseBuilder
import ru.itclover.streammachine.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.streammachine.phases.Phases.AnyExtractor
import ru.itclover.streammachine.resultmappers.ResultMappable
import ru.itclover.streammachine.transformers.{FlinkPatternMapper, RichStatefulFlatMapper, StreamSource}
import ru.itclover.streammachine.utils.UtilityTypes.ParseException
import ru.itclover.streammachine.utils.CollectionsOps.RightBiasedEither
import ru.itclover.streammachine.DataStreamUtils.DataStreamOps


case class PatternsSearchJob[InEvent: StreamSource, PhaseOut, OutEvent: TypeInformation](
  patterns: Seq[RawPattern],
  inputConf: InputConf[InEvent],
  outputConf: OutputConf[OutEvent],
  getResultMapper: ResultMappable[InEvent, PhaseOut, OutEvent]
) {
  val streamSrc = implicitly[StreamSource[InEvent]]
  val searchStageName = "Patterns search stage"
  val saveStageName = "Writing found patterns"

  def execute(jobUuid: String)(implicit streamEnv: StreamExecutionEnvironment): Either[Throwable, JobExecutionResult] =
    for {
      te <- inputConf.timeExtractor
      sn <- inputConf.symbolNumberExtractor
      ae <- inputConf.anyExtractor
      x <- findPatterns(patterns, inputConf, getResultMapper)(te, sn, ae)
        .map(_.name(searchStageName))
        .map(saveStream(_, outputConf))
        .map(_.name(saveStageName))
    } yield {
      streamEnv.execute(jobUuid)
    }


  def findPatterns(rawPatterns: Seq[RawPattern],
                   inputConf: InputConf[InEvent],
                   getResultMappers: ResultMappable[InEvent, PhaseOut, OutEvent])
                  (implicit extractTime: TimeExtractor[InEvent],
                   extractSymbol: SymbolNumberExtractor[InEvent],
                   extractAny: AnyExtractor[InEvent]): Either[Throwable, DataStream[OutEvent]] = {
    val phasesOrErr = preparePatterns(rawPatterns)
    for {
      stream <- streamSrc.createStream
      isTerminal <- streamSrc.getTerminalCheck
      phases <- phasesOrErr.leftMap(errs => ParseException(errs.mkString("\n\n"))).toEither
    } yield {
      val patternMappers = phases.zip(rawPatterns).map({ case (phase, raw) =>
        val resultsMapper = getResultMappers(raw).asInstanceOf[ResultMapper[InEvent, Any, OutEvent]]
        new FlinkPatternMapper(phase, resultsMapper, inputConf.eventsMaxGapMs,
          streamSrc.emptyEvent, isTerminal).asInstanceOf[RichStatefulFlatMapper[InEvent, Any, OutEvent]]
      })
      val (serExtractAny, serPartitionFields) = (extractAny, inputConf.partitionFields) // made job code serializable
      val keyed = stream.keyBy(e => {
        serPartitionFields.map(serExtractAny(e, _)).mkString
      })
      keyed.flatMapAll(patternMappers)(implicitly[TypeInformation[OutEvent]])
    }
  }

  def saveStream(stream: DataStream[OutEvent], outputConf: OutputConf[OutEvent]): DataStreamSink[OutEvent] = {
    val outFormat = outputConf.getOutputFormat
    stream.writeUsingOutputFormat(outFormat)
  }

  import cats.Traverse
  import cats.implicits._

  def preparePatterns(patterns: Seq[RawPattern])
                     (implicit timeExtractor: TimeExtractor[InEvent],
                      symbolNumberExtractor: SymbolNumberExtractor[InEvent]): Validated[List[String], Seq[PhaseParser[InEvent, _, _]]] = {
    Traverse[List].traverse(patterns.toList)(p =>
      Validated.fromEither(PhaseBuilder.build[InEvent](p.sourceCode)).leftMap(List(_))
    )
  }

}
