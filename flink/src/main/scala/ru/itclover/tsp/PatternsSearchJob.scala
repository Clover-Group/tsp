package ru.itclover.tsp

import cats.Traverse
import cats.data.Validated
import cats.implicits._
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.{Time => WindowingTime}
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.{Window => FlinkWindow}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import ru.itclover.tsp.core.IncidentInstances.semigroup
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.aggregators.TimestampsAdderPattern
import ru.itclover.tsp.core.io.{BasicDecoders, Extractor, TimeExtractor}
import ru.itclover.tsp.core.{Incident, RawPattern, _}
import ru.itclover.tsp.dsl.{ASTPatternGenerator, AnyState, PatternMetadata}
import ru.itclover.tsp.io.input.KafkaInputConf
import ru.itclover.tsp.io.output.{KafkaOutputConf, OutputConf}
import ru.itclover.tsp.mappers._
import ru.itclover.tsp.transformers.SparseRowsDataAccumulator
import ru.itclover.tsp.utils.Bucketizer
import ru.itclover.tsp.utils.Bucketizer.Bucket
import ru.itclover.tsp.utils.DataStreamOps.DataStreamOps
import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, InvalidPatternsCode}

import scala.reflect.ClassTag

case class PatternsSearchJob[In: TypeInformation, InKey, InItem](
  source: StreamSource[In, InKey, InItem],
  fields: Set[InKey],
  decoders: BasicDecoders[InItem]
) {
  // TODO: Restore InKey as a type parameter

  import PatternsSearchJob._
  import decoders._
  import source.{eventCreator, keyCreator, kvExtractor}

  def patternsSearchStream[OutE: TypeInformation, OutKey, S](
    rawPatterns: Seq[RawPattern],
    outputConf: OutputConf[OutE],
    resultMapper: RichMapFunction[Incident, OutE]
  ): Either[ConfigErr, (Seq[RichPattern[In, Segment, AnyState[Segment]]], Vector[DataStreamSink[OutE]])] = {
    import source.{idxExtractor, transformedExtractor, transformedTimeExtractor}
    preparePatterns[In, S, InKey, InItem](
      rawPatterns,
      source.fieldToEKey,
      source.conf.defaultToleranceFraction.getOrElse(0),
      source.conf.eventsMaxGapMs,
      source.fieldsClasses.map { case (s, c) => s -> ClassTag(c) }.toMap
    ).map { patterns =>
      val forwardFields = outputConf.forwardedFieldsIds.map(id => (id, source.fieldToEKey(id)))
      val useWindowing = !source.conf.isInstanceOf[KafkaInputConf]
      val incidents = cleanIncidentsFromPatterns(patterns, forwardFields, useWindowing)
      val mapped = incidents.map(x => x.map(resultMapper))
      (patterns, mapped.map(m => saveStream(m, outputConf)))
    }
  }

  def cleanIncidentsFromPatterns(
    richPatterns: Seq[RichPattern[In, Segment, AnyState[Segment]]],
    forwardedFields: Seq[(Symbol, InKey)],
    useWindowing: Boolean
  ): Vector[DataStream[Incident]] =
    for {
      sourceBucket <- bucketizePatterns(richPatterns, source.conf.numParallelSources.getOrElse(1))
      stream = source.createStream
      patternsBucket <- bucketizePatterns(sourceBucket.items, source.conf.patternsParallelism.getOrElse(1))
    } yield {
      import source.timeExtractor
      val singleIncidents = incidentsFromPatterns(
        applyTransformation(stream.assignAscendingTimestamps(timeExtractor(_).toMillis)),
        patternsBucket.items,
        forwardedFields,
        useWindowing
      )
      if (source.conf.defaultEventsGapMs > 0L) reduceIncidents(singleIncidents) else singleIncidents
    }

  def incidentsFromPatterns[T, S: ClassTag](
    stream: DataStream[In],
    patterns: Seq[RichPattern[In, Segment, S]],
    forwardedFields: Seq[(Symbol, InKey)],
    useWindowing: Boolean
  ): DataStream[Incident] = {

    import source.{transformedExtractor, transformedTimeExtractor => timeExtractor}

    log.debug("incidentsFromPatterns started")

    val mappers: Seq[PatternProcessor[In, S, Segment, Incident]] = patterns.map {
      case ((pattern, meta), rawP) =>
        val allForwardFields = forwardedFields ++ rawP.forwardedFields.map(id => (id, source.fieldToEKey(id)))
        val toIncidents = ToIncidentsMapper(
          rawP.id,
          allForwardFields.map { case (id, k) => id.toString.tail -> k },
          rawP.payload.toSeq,
          if (meta.sumWindowsMs > 0L) meta.sumWindowsMs else source.conf.defaultEventsGapMs,
          source.conf.partitionFields.map(source.fieldToEKey)
        )
        PatternProcessor[In, S, Segment, Incident](
          pattern,
          meta.sumWindowsMs,
          toIncidents.apply,
          source.conf.eventsMaxGapMs
        )(timeExtractor, source.idxExtractor) //.asInstanceOf[StatefulFlatMapper[In, S, Incident]]
    }
    val keyedStream = stream
      .assignAscendingTimestamps(timeExtractor(_).toMillis)
      .keyBy(source.transformedPartitioner)
    val windowed =
      if (useWindowing) {
        keyedStream
          .window(
            TumblingEventTimeWindows
              .of(WindowingTime.milliseconds(source.conf.chunkSizeMs.getOrElse(900000)))
              .asInstanceOf[WindowAssigner[In, FlinkWindow]]
          )
      } else {
        keyedStream
          .window(GlobalWindows.create().asInstanceOf[WindowAssigner[In, FlinkWindow]])
          .trigger(CountTrigger.of[FlinkWindow](1).asInstanceOf[Trigger[In, FlinkWindow]])
      }
    val processed = windowed
      .process[Incident](
        ProcessorCombinator[In, S, Segment, Incident](mappers, timeExtractor)
      )
      .setMaxParallelism(source.conf.maxPartitionsParallelism)

    log.debug("incidentsFromPatterns finished")
    processed
  }

  def applyTransformation(dataStream: DataStream[In]): DataStream[In] = source.conf.dataTransformation match {
    case Some(_) =>
      import source.{extractor, timeExtractor}
      dataStream
        .keyBy(source.partitioner)
        .process(
          SparseRowsDataAccumulator[In, InKey, InItem, In](source.asInstanceOf[StreamSource[In, InKey, InItem]], fields)
        )
        .setParallelism(1) // SparseRowsDataAccumulator cannot work in parallel
    case _ => dataStream
  }
}

object PatternsSearchJob {
  type RichSegmentedP[E] = RichPattern[E, Segment, AnyState[Segment]]
  type RichPattern[E, T, S] = ((Pattern[E, S, T], PatternMetadata), RawPattern)

  val log = Logger("PatternsSearchJob")
  def maxPartitionsParallelism = 8192

  def preparePatterns[E, S, EKey, EItem](
    rawPatterns: Seq[RawPattern],
    fieldsIdxMap: Symbol => EKey,
    toleranceFraction: Double,
    eventsMaxGapMs: Long,
    fieldsTags: Map[Symbol, ClassTag[_]]
  )(
    implicit extractor: Extractor[E, EKey, EItem],
    getTime: TimeExtractor[E],
    idxExtractor: IdxExtractor[E] /*,
    dDecoder: Decoder[EItem, Double]*/
  ): Either[ConfigErr, List[RichPattern[E, Segment, AnyState[Segment]]]] = {

    log.debug("preparePatterns started")

    val pGenerator = ASTPatternGenerator[E, EKey, EItem]()(
      idxExtractor,
      getTime,
      extractor,
      fieldsIdxMap
    )
    val res = Traverse[List]
      .traverse(rawPatterns.toList)(
        p =>
          Validated
            .fromEither(pGenerator.build(p.sourceCode, toleranceFraction, eventsMaxGapMs, fieldsTags))
            .leftMap(err => List(s"PatternID#${p.id}, error: ${err.getMessage}"))
            .map(
              p =>
                (
                  new TimestampsAdderPattern(SegmentizerPattern(p._1))
                    .asInstanceOf[Pattern[E, AnyState[Segment], Segment]],
                  p._2
                )
            )
        // TODO@trolley813 TimeMeasurementPattern wrapper for v2.Pattern
      )
      .leftMap[ConfigErr](InvalidPatternsCode(_))
      .map(_.zip(rawPatterns))
      .toEither

    log.debug("preparePatterns finished")

    res
  }

  def bucketizePatterns[E, T, S](
    patterns: Seq[RichPattern[E, T, S]],
    parallelism: Int
  ): Vector[Bucket[RichPattern[E, T, S]]] = {

    log.debug("bucketizePatterns started")
    // import Bucketizer.WeightExtractorInstances.phasesWeightExtrator
    val patternsBuckets = if (parallelism > patterns.length) {
      log.warn(
        s"Patterns parallelism conf ($parallelism) is higher than amount of " +
        s"phases - ${patterns.length}, setting patternsParallelism to amount of phases."
      )
      Bucketizer.bucketizeByWeight(patterns, patterns.length)(
        Bucketizer.WeightExtractorInstances.phasesWeightExtractor[E, T, S]
      )
    } else {
      Bucketizer.bucketizeByWeight(patterns, parallelism)(
        Bucketizer.WeightExtractorInstances.phasesWeightExtractor[E, T, S]
      )
    }
    log.info("Patterns Buckets:\n" + Bucketizer.bucketsToString(patternsBuckets))
    log.debug("bucketizePatterns finished")
    patternsBuckets
  }

  def reduceIncidents(incidents: DataStream[Incident]) = {
    log.debug("reduceIncidents started")

    val res = incidents
      .assignAscendingTimestamps_withoutWarns(p => p.segment.from.toMillis)
      .keyBy(_.id)
      .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[Incident] {
        override def extract(element: Incident): Long = element.maxWindowMs
      }))
      .reduce { _ |+| _ }
      .name("Uniting adjacent incidents")

    log.debug("reduceIncidents finished")
    res
  }

  def saveStream[E](stream: DataStream[E], outputConf: OutputConf[E]): DataStreamSink[E] = {
    log.debug("saveStream started")
    outputConf match {
      case kafkaConf: KafkaOutputConf =>
        val producer = new FlinkKafkaProducer(kafkaConf.broker, kafkaConf.topic, kafkaConf.serializer)
          .asInstanceOf[FlinkKafkaProducer[E]] // here we know that E == Row
        val res = stream.addSink(producer)
        log.debug("saveStream finished")
        res
      case _ =>
        val outputFormat = outputConf.getOutputFormat
        val res = stream.writeUsingOutputFormat(outputFormat)
        log.debug("saveStream finished")
        res
    }
  }
}
