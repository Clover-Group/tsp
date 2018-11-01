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
import ru.itclover.tsp.core.Time.{TimeExtractor, TimeNonTransformedExtractor}
import ru.itclover.tsp.io.input.{InputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.io.output.OutputConf
import ru.itclover.tsp.dsl.{PhaseBuilder, PhaseMetadata}
import ru.itclover.tsp.phases.NumericPhases.SymbolNumberExtractor
import ru.itclover.tsp.phases.Phases.{AnyExtractor, AnyNonTransformedExtractor}
import ru.itclover.tsp.transformers.{FlinkPatternMapper, RichStatefulFlatMapper, SparseRowsDataAccumulator, StreamSource}
import ru.itclover.tsp.utils.UtilityTypes.ParseException
import ru.itclover.tsp.DataStreamUtils.DataStreamOps
import ru.itclover.tsp.core.IncidentInstances.semigroup
import PatternsSearchJob._
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.dsl.schema.RawPattern
import ru.itclover.tsp.io.EventCreator
import ru.itclover.tsp.io.Exceptions.InvalidRequest
import ru.itclover.tsp.utils.Bucketizer

object PatternsSearchJob {
  type PatternWithMeta[InEvent] = ((Pattern[InEvent, _, _], PhaseMetadata), RawPattern)
  type Phases[InEvent] = scala.Seq[PatternWithMeta[InEvent]]
  type ValidatedPhases[InEvent] = Either[Throwable, Phases[InEvent]]
}

case class PatternsSearchJob[InEvent: StreamSource, PhaseOut, OutEvent: TypeInformation](
  inputConf: InputConf[InEvent],
  outputConf: OutputConf[OutEvent],
  resultMapper: RichMapFunction[Incident, OutEvent]
)(
  implicit extractTime: TimeExtractor[InEvent],
  extractTimeNonTransformed: TimeNonTransformedExtractor[InEvent],
  extractNumber: SymbolNumberExtractor[InEvent],
  extractAny: AnyExtractor[InEvent],
  extractAnyNonTransformed: AnyNonTransformedExtractor[InEvent],
  extractKeyVal: InEvent => (Symbol, AnyRef),
  eventCreator: EventCreator[InEvent],
  eventTypeInfo: TypeInformation[InEvent]
) {

  import Bucketizer.WeightExtractorInstances.phasesWeightExtrator

  val streamSrc = implicitly[StreamSource[InEvent]]
  def searchStageName(bucketNum: Int) = s"Patterns search and save stage in Bucket#${bucketNum}"
  def maxPartitionsParallelism = 8192
  val log = Logger("PatternsSearchJob")

  def preparePhases(rawPatterns: Seq[RawPattern]): ValidatedPhases[InEvent] = {
    Traverse[List]
      .traverse(rawPatterns.toList)(
        p =>
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

    val sourcesNum = inputConf.numParallelSources.getOrElse(1)
    val sourcesBuckets = if (sourcesNum > phases.length) {
      log.warn(s"Num. of parallel source branches conf ($sourcesNum) is higher than amount of " +
        s"phases - ${phases.length}, setting numParallelSources to amount of phases.")
      Bucketizer.bucketizeByWeight(phases, phases.length)
    } else {
      Bucketizer.bucketizeByWeight(phases, sourcesNum)
    }
    sourcesBuckets foreach { bucket =>
      findAndSavePatterns(bucket.items)
    }
    Either.catchNonFatal(streamEnv.execute(jobUuid))
  }

  def findAndSavePatterns(phases: Phases[InEvent]): Either[Throwable, Seq[DataStreamSink[OutEvent]]] = {
    for {
      stream     <- streamSrc.createStream
      isTerminal <- streamSrc.getTerminalCheck
      _          <- checkConfigs
    } yield {
      if (inputConf.parallelism.isDefined) stream.setParallelism(inputConf.parallelism.get)
      val patternsThreadsNum = inputConf.patternsParallelism.getOrElse(1)
      val patternsBuckets = if (patternsThreadsNum > phases.length) {
        log.warn(s"Patterns parallelism conf ($patternsThreadsNum) is higher than amount of " +
          s"phases - ${phases.length}, setting patternsParallelism to amount of phases.")
        Bucketizer.bucketizeByWeight(phases, phases.length)
      } else {
        Bucketizer.bucketizeByWeight(phases, patternsThreadsNum)
      }
      log.info("Patterns Buckets:\n" + Bucketizer.bucketsToString(patternsBuckets))
      log.info(s"Source data transformation is ${inputConf.dataTransformation}")
      val emptyEvent = inputConf.dataTransformation match {
        case Some(NarrowDataUnfolding(_, _, _, _)) => SparseRowsDataAccumulator.emptyEvent(inputConf)
        case _ => streamSrc.emptyEvent
      }
      val patternMappersBuckets = patternsBuckets.map(_.items.map {
        case ((phase, metadata), raw) =>
          val incidentsRM = new ToIncidentsResultMapper(
            raw,
            if (metadata.maxWindowMs > 0L) metadata.maxWindowMs else inputConf.defaultEventsGapMs,
            outputConf.forwardedFields ++ raw.forwardedFields,
            inputConf.partitionFields
          ).asInstanceOf[ResultMapper[InEvent, Any, Incident]]
          new FlinkPatternMapper(phase, incidentsRM, inputConf.eventsMaxGapMs, emptyEvent, isTerminal)
            .asInstanceOf[RichStatefulFlatMapper[InEvent, Any, Incident]]
      })
      for { mappers <- patternMappersBuckets } yield {
        val (serExtractAny, serExtractAnyNonTransformed, serPartitionFields) = (extractAny, extractAnyNonTransformed, inputConf.partitionFields) // made job code serializable
        val incidents = stream
          .keyBy(e => serPartitionFields.map(serExtractAnyNonTransformed(e, _)).mkString)
          .flatMapIf(
            inputConf.dataTransformation.exists(
              x => x.isInstanceOf[NarrowDataUnfolding] || x.isInstanceOf[WideDataFilling]
            ),
            SparseRowsDataAccumulator(inputConf)
          )
          .keyBy(e => serPartitionFields.map(serExtractAny(e, _)).mkString)
          .flatMapAll(mappers)
          .setMaxParallelism(maxPartitionsParallelism)
          .name("Searching for incidents")

        // Aggregate contiguous incidents in one big pattern (if configured)
        val results = if (inputConf.defaultEventsGapMs > 0L) { // todo configure via request & move it somewhere else
          incidents
            .assignAscendingTimestamps(p => p.segment.from.toMillis)
            .keyBy(e => e.id + e.partitionFields.values.mkString)
            .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[Incident] {
              override def extract(element: Incident): Long = element.maxWindowMs
            }))
            .reduce { _ |+| _ }
            .name("Uniting adjacent incidents")
            .map(resultMapper)
        } else {
          incidents.map(resultMapper)
        }

        results
          .name("Mapping results")
          .writeUsingOutputFormat(outputConf.getOutputFormat)
          .name("Saving incidents")
      }
    }
  }

  def checkConfigs: Either[Throwable, Unit] = for {
    _ <- Either.cond(
      inputConf.parallelism.getOrElse(1) > 0,
      Unit,
      InvalidRequest(s"Input conf parallelism cannot be lower than 1.") // .. Specific exception
    )
    _ <- Either.cond(
      inputConf.patternsParallelism.getOrElse(1) > 0,
      Unit,
      InvalidRequest(s"Input conf patternsParallelism cannot be lower than 1.")
    )
    _ <- Either.cond(
      outputConf.parallelism.getOrElse(1) > 0,
      Unit,
      InvalidRequest(s"Output conf parallelism cannot be lower than 1.")
    )
  } yield Unit
}
