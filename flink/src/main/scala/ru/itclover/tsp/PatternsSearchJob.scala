package ru.itclover.tsp

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SessionWindowTimeGapExtractor}
import cats.data.Validated
import cats.Traverse
import cats.implicits._
import ru.itclover.tsp.core.{Incident, Pattern, Window}
import ru.itclover.tsp.core.Time.TimeExtractor
import ru.itclover.tsp.io.input.{InputConf, RawPattern}
import ru.itclover.tsp.io.output.OutputConf
import ru.itclover.tsp.dsl.{PhaseBuilder, PhaseMetadata}
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.tsp.phases.Phases.AnyExtractor
import ru.itclover.tsp.transformers.{FlinkPatternMapper, RichStatefulFlatMapper, StreamSource}
import ru.itclover.tsp.utils.UtilityTypes.ParseException
import ru.itclover.tsp.DataStreamUtils.DataStreamOps
import ru.itclover.tsp.core.IncidentInstances.semigroup
import PatternsSearchJob._

object PatternsSearchJob {
  type Phases[InEvent] = scala.Seq[((Pattern[InEvent, _, _], PhaseMetadata), RawPattern)]
  type ValidatedPhases[InEvent] = Either[Throwable, Phases[InEvent]]
}

case class PatternsSearchJob[InEvent: StreamSource, PhaseOut, OutEvent: TypeInformation](
  inputConf: InputConf[InEvent],
  outputConf: OutputConf[OutEvent],
  resultMapper: RichMapFunction[Incident, OutEvent]
)(
  implicit extractTime: TimeExtractor[InEvent],
  extractNumber: SymbolNumberExtractor[InEvent],
  extractAny: AnyExtractor[InEvent]
) {

  val streamSrc = implicitly[StreamSource[InEvent]]
  val searchStageName = "Patterns search stage"
  val saveStageName = "Writing found patterns"

  def preparePhases(rawPatterns: Seq[RawPattern]): ValidatedPhases[InEvent] = {
    Traverse[List]
      .traverse(rawPatterns.toList)(p =>
        Validated
          .fromEither(PhaseBuilder.build[InEvent](p.sourceCode))
          .leftMap(err => List(s"PatternID#${p.id}, error: " + err))
      )
      .bimap(
        errs => ParseException(errs),
        patterns => patterns.zip(rawPatterns)
      )
      .toEither

  }

  def executeFindAndSave(phases: Phases[InEvent], jobUuid: String)(
    implicit streamEnv: StreamExecutionEnvironment
  ): Either[Throwable, JobExecutionResult] = {
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    for {
      _  <- findPatterns(phases)
        .map(_.name(searchStageName))
        .map(saveStream(_, outputConf))
        .map(_.name(saveStageName))
      result <- Either.catchNonFatal(streamEnv.execute(jobUuid))
    } yield result
  }

  def findPatterns(phases: Phases[InEvent]): Either[Throwable, DataStream[OutEvent]] = {
    for {
      stream     <- streamSrc.createStream
      isTerminal <- streamSrc.getTerminalCheck
    } yield {
      val patternMappers = phases.map({
        case ((phase, metadata), raw) =>
          val incidentsRM = new ToIncidentsResultMapper(
            raw.id,
            if (metadata.maxWindowMs > 0L) metadata.maxWindowMs else inputConf.defaultEventsGapMs,
            outputConf.forwardedFields ++ raw.forwardedFields,
            inputConf.partitionFields
          ).asInstanceOf[ResultMapper[InEvent, Any, Incident]]
          new FlinkPatternMapper(phase, incidentsRM, inputConf.eventsMaxGapMs, streamSrc.emptyEvent, isTerminal)
            .asInstanceOf[RichStatefulFlatMapper[InEvent, Any, Incident]]
      })
      val (serExtractAny, serPartitionFields) = (extractAny, inputConf.partitionFields) // made job code serializable
      val incidents = stream
        .keyBy(e => serPartitionFields.map(serExtractAny(e, _)).mkString)
        .flatMapAll(patternMappers)

      // Aggregate contiguous incidents in one big pattern (if configured)
      if (inputConf.defaultEventsGapMs > 0L) {
        incidents
          .assignAscendingTimestamps(p => p.segment.from.toMillis)
          .keyBy(e => e.id + e.partitionFields.values.mkString)
          .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[Incident] {
            override def extract(element: Incident): Long = element.maxWindowMs
          }))
          .reduce { _ |+| _ }
          .map(resultMapper)
      }
      else {
        incidents.map(resultMapper)
      }
    }
  }

  def saveStream(stream: DataStream[OutEvent], outputConf: OutputConf[OutEvent]): DataStreamSink[OutEvent] = {
    val outFormat = outputConf.getOutputFormat
    stream.writeUsingOutputFormat(outFormat).setParallelism(1)
  }
}
