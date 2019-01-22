package ru.itclover.tsp.v2

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SessionWindowTimeGapExtractor}
import cats.data.Validated
import cats.{Foldable, Functor, Monad, Traverse}
import cats.implicits._
import ru.itclover.tsp.core.{Incident, RawPattern, Window}
import ru.itclover.tsp.v2.Pattern
import ru.itclover.tsp.io.input.{InputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.io.output.OutputConf
import ru.itclover.tsp.dsl.PatternMetadata
import ru.itclover.tsp.dsl.v2.ASTPatternGenerator
import ru.itclover.tsp.mappers._
import ru.itclover.tsp.utils.UtilityTypes.ParseException
import ru.itclover.tsp.core.IncidentInstances.semigroup
import cats.kernel.Semigroup
import com.typesafe.scalalogging.Logger
import org.apache.flink.types.Row
import ru.itclover.tsp.StreamSource
import ru.itclover.tsp.io._
import ru.itclover.tsp.phases.TimeMeasurementPhases.TimeMeasurementPattern
import ru.itclover.tsp.utils.Exceptions.InvalidRequest
import ru.itclover.tsp.utils.Bucketizer
import ru.itclover.tsp.utils.Bucketizer.{Bucket, WeightExtractor}
import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, InvalidPatternsCode}
import ru.itclover.tsp.dsl.v2.ASTPatternGenerator
import ru.itclover.tsp.v2.Pattern.IdxExtractor

import scala.language.higherKinds


case class PatternsSearchJob[In: IdxExtractor, InKey, InItem, F[_]: Monad, Cont[_]: Functor: Foldable](
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
  ): Either[ConfigErr, (Seq[RichPattern[In, F, Cont]], Vector[DataStreamSink[OutE]])] =
    preparePatterns[In, InKey, InItem, F, Cont](rawPatterns, source.fieldToEKey, source.conf.defaultToleranceFraction.getOrElse(0)) map { patterns =>
      val forwardFields = outputConf.forwardedFieldsIds.map(id => (id, source.fieldToEKey(id)))
      val incidents = cleanIncidentsFromPatterns(patterns, forwardFields)
      val mapped = incidents.map(x => x.map(resultMapper))
      (patterns, mapped.map(m => saveStream(m, outputConf)))
    }


  def cleanIncidentsFromPatterns(
    richPatterns: Seq[RichPattern[In, F, Cont]],
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
    patterns: Seq[RichPattern[In, F, Cont]],
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
//        PatternFlatMapper(
//          pattern,
//          toIncidents.apply,
//          source.conf.eventsMaxGapMs,
//          source.emptyEvent
//        )(timeExtractor).asInstanceOf[StatefulFlatMapper[In, Any, Incident]]
      ???
    }
    stream
      .keyBy(source.partitioner)
      .flatMap(new FlatMappersCombinator[In, Any, Incident](mappers))
      .setMaxParallelism(source.conf.maxPartitionsParallelism)
  }
}

object PatternsSearchJob {
  type RichPattern[E, F[_], Cont[_]] = ((Pattern[E, _, _, F, Cont], PatternMetadata), RawPattern)

  val log = Logger("PatternsSearchJob")
  def maxPartitionsParallelism = 8192

  def preparePatterns[E: TimeExtractor: IdxExtractor, EKey, EItem, F[_]: Monad, Cont[_]: Functor: Foldable](
    rawPatterns: Seq[RawPattern],
    fieldsIdxMap: Symbol => EKey,
    toleranceFraction: Double
  )(
    implicit extractor: Extractor[E, EKey, EItem],
    dDecoder: Decoder[EItem, Double]
  ): Either[ConfigErr, List[RichPattern[E, F, Cont]]] = {
    Traverse[List]
      .traverse(rawPatterns.toList)(
        p =>
          Validated
            .fromEither(ASTPatternGenerator[E, F, Cont]().build(p.sourceCode, toleranceFraction))
            .map(p => p.asInstanceOf[RichPattern[E, F, Cont]])
            .leftMap(err => List(s"PatternID#${p.id}, error: " + err))
      )
      .leftMap[ConfigErr](InvalidPatternsCode(_))
      //.map(_.zip(rawPatterns))
      .toEither
  }

  def bucketizePatterns[E, F[_], Cont[_]](patterns: Seq[RichPattern[E, F, Cont]], parallelism: Int): Vector[Bucket[RichPattern[E, F, Cont]]] = {
    import Bucketizer.WeightExtractorInstances.newPhasesWeightExtractor
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
