package ru.itclover.tsp

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SessionWindowTimeGapExtractor}
import cats.data.Validated
import cats.{Monad, Traverse}
import cats.implicits._
import ru.itclover.tsp.core.{Incident, Pattern, Window}
import ru.itclover.tsp.io.input.{InputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.io.output.OutputConf
import ru.itclover.tsp.dsl.{PhaseBuilder, PhaseMetadata}
import ru.itclover.tsp.transformers._
import ru.itclover.tsp.utils.UtilityTypes.ParseException
import ru.itclover.tsp.core.IncidentInstances.semigroup
import cats.kernel.Semigroup
import com.typesafe.scalalogging.Logger
import org.apache.flink.types.Row
import ru.itclover.tsp.dsl.schema.RawPattern
import ru.itclover.tsp.io._
import ru.itclover.tsp.utils.Exceptions.InvalidRequest
import ru.itclover.tsp.utils.Bucketizer
import ru.itclover.tsp.utils.Bucketizer.{Bucket, WeightExtractor}
import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, InvalidPatternsCode}
import scala.language.higherKinds


case class PatternsSearchJob[In, InKey, InItem](
  source: StreamSource[In, InKey, InItem],
  decoders: BasicDecoders[InItem]
) {

  import source.{timeExtractor, extractor}
  import decoders._
  import PatternsSearchJob._

  def patternsSearchStream[OutE: TypeInformation, OutKey](
    patterns: Seq[RawPattern],
    outputConf: OutputConf[OutE],
    resultMapper: RichMapFunction[Incident, OutE]
  ): Either[ConfigErr, Vector[DataStreamSink[OutE]]] = for {
    incidents <- cleanIncidentsFromRawPatterns(patterns, outputConf.forwardedFieldsIds.map(source.fieldToEKey))
    mapped = incidents.map(_.map(resultMapper))
  } yield mapped.map(m => saveStream(m, outputConf))


  def cleanIncidentsFromRawPatterns(
    patterns: Seq[RawPattern],
    forwardedFields: Seq[InKey]
  ): Either[ConfigErr, Vector[DataStream[Incident]]] =
    preparePatterns(patterns, source.fieldToEKey) map { richPatterns =>
      for {
        sourceBuckets   <- bucketizePatterns(richPatterns, source.conf.numParallelSources.getOrElse(1))
        patternsBuckets <- bucketizePatterns(sourceBuckets.items, source.conf.patternsParallelism.getOrElse(1))
      } yield
        reduceIncidents(
          incidentsFromPatterns(source.createStream, patternsBuckets.items, forwardedFields)
        )
    }

  def incidentsFromPatterns(
    stream: DataStream[In],
    patterns: Seq[RichPattern[In]],
    forwardedFields: Seq[InKey]
  ): DataStream[Incident] = {
    val mappers = patterns.map {
      case ((phase, metadata), raw) =>
        val incidentsRM = new ToIncidentsResultMapper(
            raw,
            if (metadata.maxWindowMs > 0L) metadata.maxWindowMs else source.conf.defaultEventsGapMs,
            forwardedFields ++ raw.forwardedFields.map(source.fieldToEKey),
            source.conf.partitionFields.map(source.fieldToEKey)
          ).asInstanceOf[ResultMapper[In, Any, Incident]]
        new FlinkPatternMapper(phase, incidentsRM, source.conf.eventsMaxGapMs, source.emptyEvent, source.isEventTerminal)
          .asInstanceOf[RichStatefulFlatMapper[In, Any, Incident]]
    }
    stream.keyBy(source.partitioner).flatMap(new FlatMappersCombinator[In, Any, Incident](mappers))
  }
}

object PatternsSearchJob {
  type RichPattern[E] = ((Pattern[E, _, _], PhaseMetadata), RawPattern)

  val log = Logger("PatternsSearchJob")
  def maxPartitionsParallelism = 8192

  def preparePatterns[E: TimeExtractor, EKey, EItem](
    rawPatterns: Seq[RawPattern],
    fieldsIdxMap: Symbol => EKey
  )(
    implicit extractor: Extractor[E, EKey, EItem],
    dDecoder: Decoder[EItem, Double]
  ): Either[ConfigErr, List[RichPattern[E]]] = {
    Traverse[List]
      .traverse(rawPatterns.toList)(
        p =>
          Validated
            .fromEither(PhaseBuilder.build[E, EKey, EItem](p.sourceCode, fieldsIdxMap.apply))
            .leftMap(err => List(s"PatternID#${p.id}, error: " + err))
      )
      .leftMap[ConfigErr](InvalidPatternsCode(_))
      .map(_.zip(rawPatterns))
      .toEither
  }

  def bucketizePatterns[E](patterns: Seq[RichPattern[E]], parallelism: Int): Vector[Bucket[RichPattern[E]]] = {
    import Bucketizer.WeightExtractorInstances.phasesWeightExtrator
    val patternsBuckets = if (parallelism > patterns.length) {
      log.warn(
        s"Patterns parallelism conf ($parallelism) is higher than amount of " +
          s"phases - ${patterns.length}, setting patternsParallelism to amount of phases."
      )
      Bucketizer.bucketizeByWeight(patterns, patterns.length)
    } else {
      Bucketizer.bucketizeByWeight(patterns, parallelism)
    }
    log.info("Patterns Buckets:\n" + Bucketizer.bucketsToString(patternsBuckets))
    patternsBuckets
  }

  def reduceIncidents(incidents: DataStream[Incident]) = {
    incidents
      .assignAscendingTimestamps(p => p.segment.from.toMillis)
      .keyBy(e => e.id + e.partitionFields.values.mkString)
      .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[Incident] {
        override def extract(element: Incident): Long = element.maxWindowMs
      }))
      .reduce { _ |+| _ }
      .name("Uniting adjacent incidents")
  }

  // todo .. StrSink here?
  def saveStream[E, EKey](stream: DataStream[E], outputConf: OutputConf[E]) = {
    stream.writeUsingOutputFormat(outputConf.getOutputFormat)
  }
}
