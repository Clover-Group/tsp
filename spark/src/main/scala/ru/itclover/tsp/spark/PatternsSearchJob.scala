package ru.itclover.tsp.spark


import cats.Traverse
import cats.data.Validated
import cats.implicits._
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrameWriter, Dataset, Encoder}
import org.apache.spark.streaming.Milliseconds
import ru.itclover.tsp.core.IncidentInstances.semigroup
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.aggregators.TimestampsAdderPattern
import ru.itclover.tsp.core.io.{BasicDecoders, Extractor, TimeExtractor}
import ru.itclover.tsp.core.optimizations.Optimizer
import ru.itclover.tsp.core.{Incident, RawPattern, _}
import ru.itclover.tsp.dsl.{ASTPatternGenerator, AnyState, PatternMetadata}
import ru.itclover.tsp.spark.utils._
import ru.itclover.tsp.spark.io.{JDBCOutputConf, OutputConf}
import ru.itclover.tsp.spark.utils.ErrorsADT.{ConfigErr, InvalidPatternsCode}
//import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, InvalidPatternsCode}
import ru.itclover.tsp.spark.utils.EncoderInstances._

import scala.reflect.ClassTag

case class PatternsSearchJob[In, InKey, InItem](
                                                                  source: StreamSource[In, InKey, InItem],
                                                                  fields: Set[InKey],
                                                                  decoders: BasicDecoders[InItem]
                                                                ) {
  // TODO: Restore InKey as a type parameter

  import PatternsSearchJob._
  import decoders._
  // import source.{eventCreator, keyCreator, kvExtractor}

  def patternsSearchStream[OutE: Encoder, OutKey, S](
                                                              rawPatterns: Seq[RawPattern],
                                                              outputConf: OutputConf[OutE],
                                                              resultMapper: Incident => OutE,
                                                            ): Either[ConfigErr, (Seq[RichPattern[In, Segment, AnyState[Segment]]], DataFrameWriter[OutE])] = {
    import source.{idxExtractor, transformedExtractor, transformedTimeExtractor}
    preparePatterns[In, S, InKey, InItem](
      rawPatterns,
      source.fieldToEKey,
      source.conf.defaultToleranceFraction.getOrElse(0),
      source.conf.eventsMaxGapMs,
      source.fieldsClasses.map { case (s, c) => s -> ClassTag(c) }.toMap
    ).map { patterns =>
      val forwardFields = outputConf.forwardedFieldsIds.map(id => (id, source.fieldToEKey(id)))
      val useWindowing = true // !source.conf.isInstanceOf[KafkaInputConf]
      val incidents = cleanIncidentsFromPatterns(patterns, forwardFields, useWindowing)
      val mapped = incidents.map(resultMapper)
      (patterns,  saveStream(mapped, outputConf))
    }
  }

  def cleanIncidentsFromPatterns(
                                  richPatterns: Seq[RichPattern[In, Segment, AnyState[Segment]]],
                                  forwardedFields: Seq[(Symbol, InKey)],
                                  useWindowing: Boolean
                                ): Dataset[Incident] = {
    import source.timeExtractor
    val stream = source.createStream
    val singleIncidents = incidentsFromPatterns(
      applyTransformation(stream/*.assignAscendingTimestamps(timeExtractor(_).toMillis)*/),
      richPatterns,
      forwardedFields,
      useWindowing
    )
    if (source.conf.defaultEventsGapMs > 0L) reduceIncidents(singleIncidents) else singleIncidents
  }

  def incidentsFromPatterns[T](
                                stream: Dataset[In],
                                patterns: Seq[RichPattern[In, Segment, AnyState[Segment]]],
                                forwardedFields: Seq[(Symbol, InKey)],
                                useWindowing: Boolean
                              ): Dataset[Incident] = {

    import source.{transformedExtractor, idxExtractor, transformedTimeExtractor => timeExtractor}

    log.debug("incidentsFromPatterns started")

    val mappers: Seq[PatternProcessor[In, Optimizer.S[Segment], Incident]] = patterns.map {
      case ((pattern, meta), rawP) =>
        val allForwardFields = forwardedFields ++ rawP.forwardedFields.map(id => (id, source.fieldToEKey(id)))

        val toIncidents = ToIncidentsMapper(
          rawP.id,
          allForwardFields.map { case (id, k) => id.toString.tail -> k },
          rawP.payload.toSeq,
          if (meta.sumWindowsMs > 0L) meta.sumWindowsMs else source.conf.defaultEventsGapMs,
          source.conf.partitionFields.map(source.fieldToEKey)
        )

        val optimizedPattern = new Optimizer[In].optimize(pattern)

        val incidentPattern = MapWithContextPattern(optimizedPattern)(toIncidents.apply)

        PatternProcessor[In, Optimizer.S[Segment], Incident](
          incidentPattern,
          source.conf.eventsMaxGapMs
        )
    }
    val keyedDataset = stream//keyBy.(source.transformedPartitioner)
    val windowed =
      if (useWindowing) {
        keyedDataset
          //.window(Milliseconds(source.conf.chunkSizeMs.getOrElse(900000)))
      } else {
        keyedDataset
      }
    val processed = windowed
      .flatMap[Incident](
        (x: In) => ProcessorCombinator(mappers, timeExtractor).process(List(x))
      )
      //.setMaxParallelism(source.conf.maxPartitionsParallelism)

    log.debug("incidentsFromPatterns finished")
    processed
  }

  // TODO: Remove that stub
  def applyTransformation(stream: Dataset[In]): Dataset[In] = stream

//  def applyTransformation(stream: Dataset[In]): Dataset[In] = source.conf.dataTransformation match {
//    case Some(_) =>
//      import source.{extractor, timeExtractor}
//      Dataset
//        .keyBy(source.partitioner)
//        .process(
//          SparseRowsDataAccumulator[In, InKey, InItem, In](source.asInstanceOf[StreamSource[In, InKey, InItem]], fields)
//        )
//        .setParallelism(1) // SparseRowsDataAccumulator cannot work in parallel
//    case _ => stream
//  }
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
      )
      .leftMap[ConfigErr](InvalidPatternsCode(_))
      .map(_.zip(rawPatterns))
      .toEither

    log.debug("preparePatterns finished")

    res
  }

  def reduceIncidents(incidents: Dataset[Incident]): Dataset[Incident] = {
    log.debug("reduceIncidents started")

    val res = incidents
      //.assignAscendingTimestamps_withoutWarns(p => p.segment.from.toMillis)
      //.keyBy(_.id)
//      .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[Incident] {
//        override def extract(element: Incident): Long = element.maxWindowMs
//      }))
//      .reduce { _ |+| _ }
//      .name("Uniting adjacent incidents")

    log.debug("reduceIncidents finished")
    res
  }

  def saveStream[E](stream: Dataset[E], outputConf: OutputConf[E]): DataFrameWriter[E] = {
    log.debug("saveStream started")
    outputConf match {
//      case kafkaConf: KafkaOutputConf =>
//        // TODO: Kafka
//        log.debug("saveStream finished")
//        res
//
//      case redisConf: RedisOutputConf =>
//        // TODO: Redis
//        log.debug("saveStream finished")
//        res

      case jdbcConf: JDBCOutputConf =>
        val res = stream.write
          .format("jdbc")
            .option("url", jdbcConf.jdbcUrl)
            .option("dbtable", jdbcConf.tableName)
            .option("user", jdbcConf.userName.getOrElse(""))
            .option("password", jdbcConf.password.getOrElse(""))
        log.debug("saveStream finished")
        res
    }
  }
}

