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
import ru.itclover.tsp.core.{Incident, Pattern, RawPattern, Window}
import ru.itclover.tsp.io.input.{InputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.io.output.OutputConf
import ru.itclover.tsp.dsl.{PatternMetadata, PhaseBuilder}
import ru.itclover.tsp.mappers._
import ru.itclover.tsp.utils.UtilityTypes.ParseException
import ru.itclover.tsp.core.IncidentInstances.semigroup
import cats.kernel.Semigroup
import com.typesafe.scalalogging.Logger
import org.apache.flink.types.Row
import ru.itclover.tsp.io._
import ru.itclover.tsp.phases.TimeMeasurementPhases.TimeMeasurementPattern
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
    rawPatterns: Seq[RawPattern],
    outputConf: OutputConf[OutE],
    resultMapper: RichMapFunction[Incident, OutE]
  ): Either[ConfigErr, (Seq[RichPattern[In]], Vector[DataStreamSink[OutE]])] =
    preparePatterns(rawPatterns, source.fieldToEKey) map { patterns =>
      val forwardFields = outputConf.forwardedFieldsIds.map(id => (id, source.fieldToEKey(id)))
      val incidents = cleanIncidentsFromPatterns(patterns, forwardFields)
      val mapped = incidents.map(x => x.map(resultMapper))
      (patterns, mapped.map(m => saveStream(m, outputConf)))
    }


  def cleanIncidentsFromPatterns(
    richPatterns: Seq[RichPattern[In]],
    forwardedFields: Seq[(Symbol, InKey)]
  ): Vector[DataStream[Incident]] =
    for {
      sourceBucket   <- bucketizePatterns(richPatterns, source.conf.numParallelSources.getOrElse(1))
      stream         =  source.createStream
      patternsBucket <- bucketizePatterns(sourceBucket.items, source.conf.patternsParallelism.getOrElse(1))
    } yield {
      val singleIncidents = incidentsFromPatterns(stream, patternsBucket.items, forwardedFields)
      if (source.conf.defaultEventsGapMs > 0L) reduceIncidents(singleIncidents) else singleIncidents
    }

  def incidentsFromPatterns(
    stream: DataStream[In],
    patterns: Seq[RichPattern[In]],
    forwardedFields: Seq[(Symbol, InKey)]
  ): DataStream[Incident] = {
    val mappers: Seq[StatefulFlatMapper[In, Any, Incident]] = patterns.map {
      case ((pattern, meta), rawP) =>
        val allForwardFields = forwardedFields ++ rawP.forwardedFields.map(id => (id, source.fieldToEKey(id)))
        val toIncidents = ToIncidentsMapper(
          rawP.id,
          allForwardFields.map { case (id, k) => id.toString.tail -> k },
          rawP.payload.toSeq,
          if (meta.maxWindowMs > 0L) meta.maxWindowMs else source.conf.defaultEventsGapMs,
          source.conf.partitionFields.map(source.fieldToEKey)
        )
        PatternFlatMapper(
          pattern,
          toIncidents.apply,
          source.conf.eventsMaxGapMs,
          source.emptyEvent
        )(timeExtractor).asInstanceOf[StatefulFlatMapper[In, Any, Incident]]
    }
    stream
      .keyBy(source.partitioner)
      .flatMap(new FlatMappersCombinator[In, Any, Incident](mappers))
      .setMaxParallelism(source.conf.maxPartitionsParallelism)
  }
}

object PatternsSearchJob {
  type RichPattern[E] = ((Pattern[E, _, Segment], PatternMetadata), RawPattern)

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
            .map(pat => (TimeMeasurementPattern(pat._1, p.id, p.sourceCode), pat._2))
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
      .keyBy(_.id)
      .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[Incident] {
        override def extract(element: Incident): Long = element.maxWindowMs
      }))
      .reduce { _ |+| _ }
      .name("Uniting adjacent incidents")
  }

  def saveStream[E](stream: DataStream[E], outputConf: OutputConf[E]) = {
    stream.writeUsingOutputFormat(outputConf.getOutputFormat)
  }
}
