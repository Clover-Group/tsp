package ru.itclover.tsp.streaming

import cats.Traverse
import cats.data.Validated
import cats.effect.IO
import cats.implicits.catsSyntaxSemigroup
import com.typesafe.scalalogging.Logger
import fs2.Chunk
import ru.itclover.tsp.StreamSource
import ru.itclover.tsp.core.IncidentInstances.semigroup
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.aggregators.TimestampsAdderPattern
import ru.itclover.tsp.core.{Incident, Pattern, RawPattern, Segment, SegmentizerPattern}
import ru.itclover.tsp.core.io.{BasicDecoders, Extractor, TimeExtractor}
import ru.itclover.tsp.core.optimizations.Optimizer
import ru.itclover.tsp.dsl.{ASTPatternGenerator, AnyState, PatternFieldExtractor, PatternMetadata}
import ru.itclover.tsp.streaming.PatternsSearchJob.{RichPattern, preparePatterns, reduceIncidents}
import ru.itclover.tsp.streaming.checkpointing.CheckpointingService
import ru.itclover.tsp.streaming.io.{KafkaInputConf, OutputConf}
import ru.itclover.tsp.streaming.mappers.{
  MapWithContextPattern,
  PatternProcessor,
  PatternsToRowMapper,
  ProcessorCombinator,
  ToIncidentsMapper
}
import ru.itclover.tsp.streaming.transformers.SparseRowsDataAccumulator
import ru.itclover.tsp.streaming.utils.ErrorsADT.{ConfigErr, InvalidPatternsCode}
import ru.itclover.tsp.streaming.utils.StreamPartitionOps

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.chaining._

case class PatternsSearchJob[In, InKey, InItem](
  jobId: String,
  source: StreamSource[In, InKey, InItem],
  decoders: BasicDecoders[InItem]
) {

  def patternsSearchStream[OutE, OutKey, S](
    rawPatterns: Seq[RawPattern],
    outputConf: Seq[OutputConf[OutE]],
    resultMappers: Seq[PatternsToRowMapper[Incident, OutE]]
  ): Either[ConfigErr, (Seq[RichPattern[In, Segment, AnyState[Segment]]], fs2.Stream[IO, Unit])] = {
    import source.{idxExtractor, transformedExtractor, transformedTimeExtractor}
    preparePatterns[In, S, InKey, InItem](
      rawPatterns,
      source.fieldToEKey,
      0,
      source.conf.eventsMaxGapMs.getOrElse(60000L),
      source.transformedFieldsClasses.map { case (s, c) => s -> ClassTag(c) }.toMap,
      source.patternFields
    ).map { patterns =>
      val forwardFields = Seq.empty
      val useWindowing = !source.conf.isInstanceOf[KafkaInputConf]
      val incidents = cleanIncidentsFromPatterns(patterns, forwardFields, useWindowing)
      // val mapped = resultMappers.map(
      //   resultMapper =>
      //     incidents
      //       .chunkLimit(source.conf.processingBatchSize.getOrElse(10000))
      //       .map(_.map(resultMapper.map(_).asInstanceOf[OutE]))
      //       .flatMap(c => fs2.Stream.chunk(c))
      // )
      val saved = incidents
        .chunkLimit(source.conf.processingBatchSize.getOrElse(10000))
        .broadcastThrough(
          resultMappers
          .zip(outputConf)
          .zipWithIndex
          .map {
            case ((m, c), idx) => saveStream(jobId, c, idx).compose(applyResultMapper(m))
          }: _*)
      (patterns, saved)
    }
  }

  def applyResultMapper[E](mapper: PatternsToRowMapper[Incident, E]): fs2.Pipe[IO, Chunk[Incident], E] =
    _.map(_.map(mapper.map(_).asInstanceOf[E])).flatMap(c => fs2.Stream.chunk(c))

  def saveStream[E](uuid: String, outputConf: OutputConf[E], sinkIdx: Int): fs2.Pipe[IO, E, Unit] = _
    .chunkLimit(100)
    .flatMap { c =>
      CheckpointingService.updateCheckpointWritten(uuid, sinkIdx, c.size)
      fs2.Stream.chunk(c)
    }
    .through(outputConf.getSink)

  def incidentsFromPatterns[T](
    stream: fs2.Stream[IO, In],
    patterns: Seq[RichPattern[In, Segment, AnyState[Segment]]],
    forwardedFields: Seq[(String, InKey)],
    useWindowing: Boolean
  ): fs2.Stream[IO, Incident] = {

    import source.{transformedExtractor, idxExtractor, transformedTimeExtractor => timeExtractor}
    import decoders.decodeToAny

    //log.debug("incidentsFromPatterns started")

    val (checkpointOption, stateOption) = CheckpointingService.getCheckpointAndState(jobId)
    val mappers: Seq[PatternProcessor[In, Optimizer.S[Segment], Incident]] = patterns.map {
      case ((pattern, meta), rawP) =>
        val toIncidents = ToIncidentsMapper(
          rawP.id,
          source.fieldToEKey(source.conf.unitIdField.getOrElse("")),
          rawP.subunit.getOrElse(0),
          rawP.metadata.getOrElse(Map.empty),
          source.conf.defaultEventsGapMs.getOrElse(2000L),
          source.conf.partitionFields.map(source.fieldToEKey)
        )

        val optimizedPattern = new Optimizer[In].optimize(pattern)

        val incidentPattern = MapWithContextPattern(optimizedPattern)(toIncidents.apply)

        PatternProcessor[In, Optimizer.S[Segment], Incident](
          incidentPattern,
          source.conf.eventsMaxGapMs.getOrElse(60000L),
          stateOption.flatMap(_.states.get(rawP)).getOrElse(incidentPattern.initialState())
        )
    }
    val keyedStream = stream
      .drop(checkpointOption.map(_.readRows).getOrElse(0L))
      //.assignAscendingTimestamps(timeExtractor(_).toMillis)
      .through(StreamPartitionOps.groupBy(e => IO { source.transformedPartitioner(e) }))
    val windowed: fs2.Stream[IO, fs2.Stream[IO, Chunk[In]]] =
      if (useWindowing) {
        keyedStream
          .map {
            case (_, str) =>
              str
                .groupAdjacentBy(e => timeExtractor(e).toMillis / source.conf.chunkSizeMs.getOrElse(900000L))
                .map { case (_, chunk) => chunk }
          }
      } else {
        // For Kafka we generate windows by processing time (not event time) every 1 second,
        // so we always get the results without collecting huge window.
        keyedStream.map {
          case (_, str) =>
            str
              .groupWithin(100000, FiniteDuration(1000, TimeUnit.MILLISECONDS))
        }
      }
    val rawPatterns = patterns.map(_._2)

    val combinator = ProcessorCombinator(
      mappers,
      timeExtractor,
      (states: Seq[Optimizer.S[Segment]], size: Int) =>
        CheckpointingService.updateCheckpointRead(jobId, size.toLong, rawPatterns.zip(states).toMap)
    )
    val processed = windowed
      .map(_.map(c => combinator.process(c)))
      //.pipe(x => if (source.conf.parallelism.getOrElse(0) > 0) x.parJoin(source.conf.parallelism.get) else x.parJoinUnbounded)
      .parJoinUnbounded
      .flatMap(c => fs2.Stream.chunk(c))

    //log.debug("incidentsFromPatterns finished")
    processed
  }

  def cleanIncidentsFromPatterns(
    richPatterns: Seq[RichPattern[In, Segment, AnyState[Segment]]],
    forwardedFields: Seq[(String, InKey)],
    useWindowing: Boolean
  ): fs2.Stream[IO, Incident] = {
    val stream = source.createStream
    val singleIncidents = incidentsFromPatterns(
      applyTransformation(stream),
      richPatterns,
      forwardedFields,
      useWindowing
    )
    if (source.conf.defaultEventsGapMs.getOrElse(2000L) > 0L) reduceIncidents(singleIncidents, source.conf.parallelism) else singleIncidents
  }

  def applyTransformation(dataStream: fs2.Stream[IO, In]): fs2.Stream[IO, In] = source.conf.dataTransformation match {
    case Some(_) =>
      import source.{extractor, timeExtractor, eventCreator, kvExtractor, keyCreator, eventPrinter}

      dataStream
        .through(StreamPartitionOps.groupBy(p => IO { source.partitioner(p) }))
        .map {
          case (_, str) => {
            val acc = SparseRowsDataAccumulator[In, InKey, InItem, In](
              source.asInstanceOf[StreamSource[In, InKey, InItem]],
              source.patternFields
            )
            str.map(event => {
              acc.map(event)
            }) ++ fs2.Stream(Some(acc.getLastEvent))
          }.unNone
        }
        .parJoinUnbounded
        //.pipe(x => if (source.conf.parallelism.getOrElse(0) > 0) x.parJoin(source.conf.parallelism.get) else x.parJoinUnbounded)
    //.setParallelism(1) // SparseRowsDataAccumulator cannot work in parallel
    case _ => dataStream
  }

}

object PatternsSearchJob {
  type RichSegmentedP[E] = RichPattern[E, Segment, AnyState[Segment]]
  type RichPattern[E, T, S] = ((Pattern[E, S, T], PatternMetadata), RawPattern)

  val log = Logger("ru.itclover.tsp.streaming.PatternsSearchJob")
  def maxPartitionsParallelism = 8192

  def preparePatterns[E, S, EKey, EItem](
    rawPatterns: Seq[RawPattern],
    fieldsIdxMap: String => EKey,
    toleranceFraction: Double,
    eventsMaxGapMs: Long,
    fieldsTags: Map[String, ClassTag[_]],
    patternFields: Set[EKey]
  )(
    implicit extractor: Extractor[E, EKey, EItem],
    getTime: TimeExtractor[E],
    idxExtractor: IdxExtractor[E]
  ): Either[ConfigErr, List[RichPattern[E, Segment, AnyState[Segment]]]] = {

    given Conversion[String, EKey] = fieldsIdxMap(_)

    log.debug("preparePatterns started")

    val filteredPatterns = rawPatterns.filter(
      p => PatternFieldExtractor.extract[E, EKey, EItem](List(p))(fieldsIdxMap).subsetOf(patternFields)
    )

    val pGenerator = ASTPatternGenerator[E, EKey, EItem]()(
      idxExtractor,
      getTime,
      extractor,
      implicitly[Conversion[String, EKey]]
    )
    val res = Traverse[List]
      .traverse(filteredPatterns.toList)(
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
      .map(_.zip(filteredPatterns))
      .toEither

    log.debug("preparePatterns finished")

    res
  }

  def reduceIncidents(incidents: fs2.Stream[IO, Incident], maxParallelism: Option[Int]): fs2.Stream[IO, Incident] = {
    log.debug("reduceIncidents started")

    val res = incidents
      .through(StreamPartitionOps.groupBy(p => IO { (p.id, p.patternUnit, p.patternSubunit) }))
      //.window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[Incident] {
      //  override def extract(element: Incident): Long = element.maxWindowMs
      //}))
      .map {
        case (_, str) =>
          str.zipWithPrevious
            .flatMap {
              case (pr, c) =>
                pr match {
                  case Some(p) =>
                    val start = c.segment.from.toMillis - p.segment.to.toMillis > c.maxWindowMs
                    val chunk = if (start) {
                      log.debug(f"Starting new event series from ${c.segment.from} --- ${c.segment.to}")
                      Chunk((Some(c), true), (None, false))
                    } else {
                      log.debug(
                        f"Continuing event series with ${c.segment.from} --- ${c.segment.to}, " +
                        f"since ${c.segment.from} minus ${p.segment.to} was less than ${c.maxWindowMs} ms"
                        )
                      Chunk((Some(c), false))
                    }
                    fs2.Stream.chunk(chunk)
                  case None => {
                    log.debug(f"Starting initial event series from ${c.segment.from} --- ${c.segment.to}")
                    fs2.Stream.chunk(Chunk((Some(c), true), (None, false)))
                  }
                }
            }
            .groupAdjacentBy(_._2)
            .chunkN(2, allowFewer = true)
            .map(c => c.flatMap(_._2).map(_._1).filter(_.isDefined).map(_.get))
            .map(c => if (c.nonEmpty) Some(c.foldLeft(c.head.get)(_ |+| _)) else None)
            .unNone
        //.reduce { _ |+| _ }
      }
      .parJoinUnbounded
      //.pipe(x => if (maxParallelism.getOrElse(0) > 0) x.parJoin(maxParallelism.get) else x.parJoinUnbounded)

    //.name("Uniting adjacent incidents")

    log.debug("reduceIncidents finished")
    res
  }

  // def saveStream[E](
  //   uuid: String,
  //   streams: Seq[fs2.Stream[IO, E]],
  //   outputConfs: Seq[OutputConf[E]]
  // ): Seq[fs2.Stream[IO, Unit]] = {
  //   log.debug("saveStream started")
  //   streams.zip(outputConfs).map {
  //     case (stream, outputConf) =>
  //       stream
  //         .chunkLimit(100)
  //         .flatMap { c =>
  //           CheckpointingService.updateCheckpointWritten(uuid, c.size)
  //           fs2.Stream.chunk(c)
  //         }
  //         .through(outputConf.getSink)
  //   }
  // }
}
