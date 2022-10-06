package ru.itclover.tsp.streaming.io

import ru.itclover.tsp.RowWithIdx

/**
  * Source for anything that support JDBC connection
  *
  * @param sourceId mark to pass to sink
  * @param jdbcUrl example - "jdbc:clickhouse://localhost:8123/default?"
  * @param query SQL query for data
  * @param driverName example - "ru.yandex.clickhouse.ClickHouseDriver"
  * @param datetimeField name of datetime field, could be timestamp and regular time (will be parsed by JodaTime)
  * @param eventsMaxGapMs maximum gap by which source data will be split, i.e. result incidents will be split by these gaps
  * @param defaultEventsGapMs "typical" gap between events, used to unite nearby incidents in one (sessionization)
  * @param partitionFields fields by which data will be split and paralleled physically
  * @param userName for JDBC auth
  * @param password for JDBC auth
  * @param parallelism of source task (not recommended to chagne)
  * @param numParallelSources number of absolutely separate sources to create. Patterns also will be separated by
  *                           equal (as much as possible) buckets by the max window in pattern (TBD by sum window size)
  * @param patternsParallelism number of parallel branch nodes splitted after sink stage (node). Patterns also
  *                            separated by approx. equal buckets by the max window in pattern (TBD by sum window size)
  */
case class JDBCInputConf(
  sourceId: Int,
  jdbcUrl: String,
  query: String,
  driverName: String,
  datetimeField: Symbol,
  eventsMaxGapMs: Option[Long] = Some(60000L),
  defaultEventsGapMs: Option[Long] = Some(2000L),
  chunkSizeMs: Option[Long],
  processingBatchSize: Option[Int],
  partitionFields: Seq[Symbol],
  unitIdField: Option[Symbol] = None,
  userName: Option[String] = None,
  password: Option[String] = None,
  dataTransformation: Option[SourceDataTransformation[RowWithIdx, Symbol, Any]] = None,
  defaultToleranceFraction: Option[Double] = None,
  parallelism: Option[Int] = Some(1),
  numParallelSources: Option[Int] = Some(1),
  patternsParallelism: Option[Int] = Some(1),
  timestampMultiplier: Option[Double] = Some(1000.0)
) extends InputConf[RowWithIdx, Symbol, Any]
