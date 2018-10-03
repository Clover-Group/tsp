# Sources types

> Note: {% include types-note.md %}

- `jdbc` - anything that support JDBC connection

```scala
/**
  * Source for anything that support JDBC connection
  * @param sourceId mark to pass to sink
  * @param jdbcUrl example - "jdbc:clickhouse://localhost:8123/default?"
  * @param query SQL query
  * @param driverName example - "ru.yandex.clickhouse.ClickHouseDriver"
  * @param datetimeFiel
  * @param eventsMaxGapMs maximum gap by which source data will be split, i.e. result incidents will be split by these gaps
  * @param defaultEventsGapMs "typical" gap between events, used to unite nearby incidents in one (sessionization)
  * @param partitionFields fields by which data will be split and paralleled physically
  * @param userName for JDBC auth
  * @param password for JDBC auth
  * @param props extra configs to JDBC `DriverManager.getConnection(`
  * @param parallelism basic parallelism of all computational nodes
  * @param patternsParallelism number of parallel branch nodes after sink stage (node)
  */
case class JDBCInputConf(
  sourceId: Int,
  jdbcUrl: String,
  query: String,
  driverName: String,
  datetimeField: Symbol,
  eventsMaxGapMs: Long,
  defaultEventsGapMs: Long,
  partitionFields: Seq[Symbol],
  userName: Option[String] = None,
  password: Option[String] = None,
  props: Option[Map[String, AnyRef]] = None,
  parallelism: Option[Int] = None,
  patternsParallelism: Option[Int] = Some(2)
) extends InputConf[Row] { ... }
```

- `influxdb`

```scala
/**
  * Source for InfluxDB
  * @param sourceId simple mark to pass to sink
  * @param dbName
  * @param url to database, for example `http://localhost:8086`
  * @param query Influx SQL query
  * @param eventsMaxGapMs maximum gap by which source data will be split, i.e. result incidents will be split by these gaps
  * @param defaultEventsGapMs "typical" gap between events, used to unite nearby incidents in one (sessionization)
  * @param partitionFields fields by which data will be split and paralleled physically
  * @param datetimeField
  * @param userName for auth
  * @param password for auth
  * @param timeoutSec for DB connection
  * @param parallelism basic parallelism of all computational nodes
  * @param patternsParallelism number of parallel branch nodes after sink stage (node)
  */
case class InfluxDBInputConf(
  sourceId: Int,
  dbName: String,
  url: String,
  query: String,
  eventsMaxGapMs: Long,
  defaultEventsGapMs: Long,
  partitionFields: Seq[Symbol],
  datetimeField: Symbol = 'time,
  userName: Option[String] = None,
  password: Option[String] = None,
  timeoutSec: Option[Long] = None,
  parallelism: Option[Int] = None,
  patternsParallelism: Option[Int] = Some(2)
) extends InputConf[Row] { ... }
```