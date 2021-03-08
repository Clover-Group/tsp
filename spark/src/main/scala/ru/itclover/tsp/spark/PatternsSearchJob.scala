package ru.itclover.tsp.spark

import cats.Traverse
import cats.data.Validated
import cats.implicits._
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.storage.StorageLevel
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.aggregators.TimestampsAdderPattern
import ru.itclover.tsp.core.io.{BasicDecoders, Extractor, TimeExtractor}
import ru.itclover.tsp.core.optimizations.Optimizer
import ru.itclover.tsp.core.{Incident, RawPattern, _}
import ru.itclover.tsp.dsl.{ASTPatternGenerator, AnyState, PatternMetadata}
import ru.itclover.tsp.spark.utils._
import ru.itclover.tsp.spark.io.{InputConf, JDBCInputConf, JDBCOutputConf, KafkaInputConf, KafkaOutputConf, NewRowSchema, OutputConf}
import ru.itclover.tsp.spark.transformers.SparseRowsDataAccumulator
import ru.itclover.tsp.spark.utils.ErrorsADT.{ConfigErr, InvalidPatternsCode}
import ru.itclover.tsp.spark.utils.DataWriterWrapperImplicits._
//import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, InvalidPatternsCode}
// import ru.itclover.tsp.spark.utils.EncoderInstances._
import org.apache.spark.sql.expressions.{Window => SparkWindow}
import org.apache.spark.sql.functions.{lag, col, sum, expr}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

case class PatternsSearchJob[In: ClassTag: TypeTag, InKey, InItem](
  uuid: String,
  source: StreamSource[In, InKey, InItem],
  fields: Set[InKey],
  decoders: BasicDecoders[InItem]
) {
  // TODO: Restore InKey as a type parameter

  import PatternsSearchJob._
  import decoders._
  // import source.{eventCreator, keyCreator, kvExtractor}

  def patternsSearchStream[OutE: ClassTag, OutKey, S](
    rawPatterns: Seq[RawPattern],
    outputConf: OutputConf[OutE],
    resultMapper: Incident => OutE
  ): Either[ConfigErr, (Seq[RichPattern[In, Segment, AnyState[Segment]]], DataWriterWrapper[OutE])] = {
    import source.{idxExtractor, transformedExtractor, transformedTimeExtractor}
    source.spark.sparkContext.setJobGroup(uuid, s"TSP Job $uuid")
    preparePatterns[In, S, InKey, InItem](
      rawPatterns,
      source.fieldToEKey,
      source.conf.defaultToleranceFraction.getOrElse(0),
      source.conf.eventsMaxGapMs.getOrElse(6000L),
      source.fieldsClasses.map { case (s, c) => s -> ClassTag(c) }.toMap
    ).map { patterns =>
      val forwardFields = outputConf.forwardedFieldsIds.map(id => (id, source.fieldToEKey(id)))
      val useWindowing = true // !source.conf.isInstanceOf[KafkaInputConf]
      val incidents = cleanIncidentsFromPatterns(patterns, forwardFields, useWindowing)
      val mapped =
        incidents.map(resultMapper)(RowEncoder(rowSchemaToSchema(outputConf.rowSchema)).asInstanceOf[Encoder[OutE]])
      (patterns, saveStream(mapped, uuid, source.conf, outputConf))
    }
  }

  def cleanIncidentsFromPatterns(
    richPatterns: Seq[RichPattern[In, Segment, AnyState[Segment]]],
    forwardedFields: Seq[(Symbol, InKey)],
    useWindowing: Boolean
  ): Dataset[Incident] = {
    // import source.timeExtractor
    val stream = source.createStream
    val singleIncidents = incidentsFromPatterns(
      applyTransformation(stream /*.assignAscendingTimestamps(timeExtractor(_).toMillis)*/ )(source.spark),
      richPatterns,
      forwardedFields,
      useWindowing
    )
    source.conf match {
      case _: KafkaInputConf => singleIncidents // this must be processed upon writing
      case _ =>
        if (source.conf.defaultEventsGapMs.getOrElse(2000L) > 0L) reduceIncidents(singleIncidents)(source.spark)
        else singleIncidents
    }

  }

  def incidentsFromPatterns[T](
    stream: Dataset[In],
    patterns: Seq[RichPattern[In, Segment, AnyState[Segment]]],
    forwardedFields: Seq[(Symbol, InKey)],
    useWindowing: Boolean
  ): Dataset[Incident] = {

    import source.{transformedExtractor, idxExtractor, transformedTimeExtractor => timeExtractor}
    val s = source.spark
    import s.implicits._

    s.streams.addListener(queryListener)

    implicit val encIn: Encoder[In] = source.eventEncoder
    implicit val encSeq: Encoder[Seq[In]] = Encoders.kryo[Seq[In]](classOf[Seq[In]])
    implicit val encList: Encoder[List[In]] = Encoders.kryo[List[In]](classOf[List[In]])

    log.debug("incidentsFromPatterns started")

    val mappers: Seq[PatternProcessor[In, Optimizer.S[Segment], Incident]] = patterns.map {
      case ((pattern, meta), rawP) =>
        val allForwardFields = forwardedFields ++ rawP.forwardedFields
            .getOrElse(Seq())
            .map(id => (id, source.fieldToEKey(id)))

        val toIncidents = ToIncidentsMapper(
          rawP.id,
          allForwardFields.map { case (id, k) => id.toString.tail -> k },
          source.fieldToEKey(source.conf.unitIdField.get),
          rawP.subunit.getOrElse(0),
          rawP.payload.getOrElse(Map()).toSeq,
          if (meta.sumWindowsMs > 0L) meta.sumWindowsMs else source.conf.defaultEventsGapMs.getOrElse(2000L),
          source.conf.partitionFields.map(source.fieldToEKey)
        )

        val optimizedPattern = new Optimizer[In].optimize(pattern)

        val incidentPattern = MapWithContextPattern(optimizedPattern)(toIncidents.apply)

        PatternProcessor[In, Optimizer.S[Segment], Incident](
          incidentPattern,
          source.conf.eventsMaxGapMs.getOrElse(60000L)
        )
    }
    val keyedDataset = stream
    //.repartition(source.transformedPartitioner.map(new Column(_)):_*)
    //.partitionBy()
    val windowed: Dataset[List[In]] =
      if (useWindowing) {
        // val chunkSize = 100000
        keyedDataset
          .repartition(col("_2." + source.conf.unitIdField.get.name))
          .mapPartitions(_.grouped(1000))(encSeq)
          .map(_.toList)(encList)
      } else {
        keyedDataset
          .repartition(col("_2." + source.conf.unitIdField.get.name))
          .mapPartitions(_.grouped(1000))(encSeq)
          .map(_.toList)(encList)
      }
    val processed = windowed
      .flatMap[Incident](
        (x: List[In]) => ProcessorCombinator(mappers, timeExtractor).process(x)
      )
    //.setMaxParallelism(source.conf.maxPartitionsParallelism)

    log.debug("incidentsFromPatterns finished")
    processed
  }

  def applyTransformation(stream: Dataset[In])(implicit spark: SparkSession): Dataset[In] = {
    implicit val encIn: Encoder[In] = source.eventEncoder
    source.conf.dataTransformation match {
      case Some(_) =>
        import source.{extractor, timeExtractor, kvExtractor, eventCreator, keyCreator}
        val acc = SparseRowsDataAccumulator[In, InKey, InItem, In](source.asInstanceOf[StreamSource[In, InKey, InItem]],
          fields)
        stream
          .union(spark.createDataset(List(source.eventCreator.create(
            source.fieldsClasses.map { case (f, c) =>
              source.fieldToEKey(f) -> getNullValue(c).asInstanceOf[AnyRef]
            }
          ))))
          .coalesce(1)
          .flatMap(acc.process)
      case None => stream
    }
  }

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

  def queryListener: StreamingQueryListener = new StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      //println(s"${event.id} started")
    }

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      val rows = event.progress.sources.map(_.numInputRows).sum
      println(s"$rows rows processed")
    }

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      //println(s"${event.id} stopped")
    }
  }

  def getNullValue(c: Class[_]): AnyRef = {
    val r = c match {
    case x if x.equals(classOf[java.lang.Byte]) || x.equals(classOf[Byte]) => 0.toByte.asInstanceOf[AnyRef]
    case x if x.equals(classOf[java.lang.Short]) || x.equals(classOf[Short]) => 0.toShort.asInstanceOf[AnyRef]
    case x if x.equals(classOf[java.lang.Integer]) || x.equals(classOf[Int]) => 0.asInstanceOf[AnyRef]
    case x if x.equals(classOf[java.lang.Long]) || x.equals(classOf[Long]) => 0L.asInstanceOf[AnyRef]
    case x if x.equals(classOf[java.lang.Float]) || x.equals(classOf[Float]) => Float.NaN.asInstanceOf[AnyRef]
    case x if x.equals(classOf[java.lang.Double]) || x.equals(classOf[Double]) => Double.NaN.asInstanceOf[AnyRef]
    case x if x.equals(classOf[java.lang.String]) || x.equals(classOf[String]) => "".asInstanceOf[AnyRef]
    case _ => null
    }
    r
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
      )
      .leftMap[ConfigErr](InvalidPatternsCode(_))
      .map(_.zip(rawPatterns))
      .toEither

    log.debug("preparePatterns finished")

    res
  }

  def reduceIncidents(inc: Dataset[Incident])(implicit spark: SparkSession): Dataset[Incident] = {
    import spark.implicits._
    log.debug("reduceIncidents started")

    val incidents = inc.persist(StorageLevel.MEMORY_AND_DISK)

    if (incidents.isEmpty) {
      return incidents
    }

    // Repeat the starting entry
    val newIncidents = spark.createDataset(List(incidents.first)).union(incidents)
    val win = SparkWindow.partitionBy("patternId", "forwardedFields").orderBy("segment.from")

    val reducer = new IncidentAggregator

    val res = newIncidents
      .withColumn("curr", col("segment.from.toMillis"))
      .withColumn("prev", lag("segment.to.toMillis", 1).over(win))
      .withColumn("seriesCount", sum(expr("curr - prev > maxWindowMs").cast("int")).over(win))
      .groupBy("seriesCount")
      .agg(
        reducer(
          $"id",
          $"patternId",
          $"maxWindowMs",
          $"segment",
          $"forwardedFields",
          $"patternUnit",
          $"patternSubunit",
          $"patternPayload"
        ).as("result")
      )
      .select(
        $"result.id".as("id"),
        $"result.patternId".as("patternId"),
        $"result.maxWindowMs".as("maxWindowMs"),
        $"result.segment".as("segment"),
        $"result.forwardedFields".as("forwardedFields"),
        $"result.patternUnit".as("patternUnit"),
        $"result.patternSubunit".as("patternSubunit"),
        $"result.patternPayload".as("patternPayload")
      )
      .drop("prev", "curr", "seriesCount")

//        .map(_._2)
    //.assignAscendingTimestamps_withoutWarns(p => p.segment.from.toMillis)
    //.keyBy(_.id)
//      .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[Incident] {
//        override def extract(element: Incident): Long = element.maxWindowMs
//      }))
//      .reduce { _ |+| _ }
//      .name("Uniting adjacent incidents")

    log.debug("reduceIncidents finished")
    res.as[Incident]
  }

  def saveStream[InE, OutE, InKey, InItem](
    stream: Dataset[OutE],
    jobId: String,
    inputConf: InputConf[InE, InKey, InItem],
    outputConf: OutputConf[OutE]
  ): DataWriterWrapper[OutE] = {
    log.debug("saveStream started")
    inputConf match {

      case _: JDBCInputConf =>
        outputConf match {
          case oc: JDBCOutputConf =>
            val res = stream.write
              .format("jdbc")
              .option("url", oc.jdbcUrl)
              .option("dbtable", oc.tableName)
              .option("user", oc.userName.getOrElse(""))
              .option("password", oc.password.getOrElse(""))
            //.start()
            //.mode(SaveMode.Append)
            log.debug("saveStream finished")
            res
          case oc: KafkaOutputConf =>
            val res = stream
              .select(to_json(struct(
                oc.rowSchema.patternIdField.name,
                oc.rowSchema.appIdFieldVal._1.name,
                oc.rowSchema.fromTsField.name,
                oc.rowSchema.toTsField.name,
                oc.rowSchema.unitIdField.name,
                oc.rowSchema.subunitIdField.name
              )).as("value"))
              .write
              .format("kafka")
              .option("kafka.bootstrap.servers", oc.broker)
              .option("topic", oc.topic)
            log.debug("saveStream finished")
            res
        }
      case _: KafkaInputConf =>
        outputConf match {
          case oc: JDBCOutputConf =>
            val sink = JDBCSink(
              oc.jdbcUrl,
              oc.tableName,
              oc.driverName,
              oc.userName.getOrElse("default"),
              oc.password.getOrElse("")
            )
            val res = stream.writeStream
              .foreach(sink.asInstanceOf[ForeachWriter[OutE]])
              .trigger(Trigger.ProcessingTime("10 seconds"))
//              .format("jdbc")
//              .option("url", oc.jdbcUrl)
//              .option("dbtable", oc.tableName)
//              .option("user", oc.userName.getOrElse(""))
//              .option("password", oc.password.getOrElse(""))
            //.start()
            //.mode(SaveMode.Append)
            log.debug("saveStream finished")
            res
          case oc: KafkaOutputConf =>
            val res = stream
              .select(to_json(struct(
                oc.rowSchema.patternIdField.name,
                oc.rowSchema.appIdFieldVal._1.name,
                oc.rowSchema.fromTsField.name,
                oc.rowSchema.toTsField.name,
                oc.rowSchema.unitIdField.name,
                oc.rowSchema.subunitIdField.name
              )).as("value"))
              .writeStream
              .format("kafka")
              .option("kafka.bootstrap.servers", oc.broker)
              .option("topic", oc.topic)
              .option("checkpointLocation", s"/tmp/tsp/checkpoint/${jobId}")
            log.debug("saveStream finished")
            res
        }
    }
  }

  def rowSchemaToSchema(rowSchema: NewRowSchema): StructType = StructType(
    rowSchema.fieldDatatypes.zip(rowSchema.fieldsNames).map { case (t, n) => new StructField(n.name, t) }
  )
}
