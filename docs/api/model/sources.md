# Source types

General parameters for all sources:

- `datetimeField` (string) is the name of the column containing the timestamp of an event.  
- `partitionFields` (array of strings) are the names of columns by which the data will be partitioned (i.e., events with differing values of the `partitionedFields` will be treated as different times series).
- `unitIdField` (string) is the name of the column containing the unit ID of an event (usually included in the partition key). Unlike the `partitionFields`, the unit ID is written to the sink
- `parallelism` (integer, default 1) is the data-level parallelism per each source (i.e. the data from a JDBC source will split into chunks). Ignored for stream sources like Kafka (for obvious reasons)
- `numParallelSources` (integer, default 1) is the number of parallel sources (with the same data) to be created (i.e. SQL queries for JDBC source) 
- `patternsParallelism` (integer, default 1) is the pattern-level parallelism per each source (i.e. the source will be broadcast to several pattern processors, each containing a subset of the patterns set).
- `eventsMaxGapMs` (integer, default 60000) is the duration in milliseconds (event time, not realtime) after which (receiving no data) the current time series will break, and a new one will be commenced, starting from the next event.
- `defaultEventsGapMs` (integer, default 2000) is the duration in milliseconds for combining events (i.e. if the gap between incidents (ending of one and starting of another) is less than the specified value, then these two incidents will squash into a single one).
- `chunkSizeMs` (integer, default 900000) is the size of a chunk (for batch execution) in terms of event time (the source data will be chunked according to this parameter).

### JDBC source
- `jdbcUrl` (string) is the JDBC-standardized URL for connecting to the database (e.g. `jdbc:clickhouse://127.0.0.1:8123/platform_db?user=default` for ClickHouse)
- `query` (string) is the SQL query for obtaining data (e.g. `SELECT time, sensor1, sensor2 FROM telemetry_data`)
- `userName` and `password` (strings) are the user name and password (for overriding those specified in JDBC URL).

### Kafka source
> Note the `parallelism` and `patternsParallelism` are hard-coded to 1 for Kafka.

- `brokers` (string) is the address of the Kafka brokers (e.g. `127.0.0.1:9092;127.0.0.1:9093`)
- `topic` (string) is the name of the topic containing time-series data.
- `group` (string) is the name of the consumer group for reading data.

### InfluxDB source 
TODO

For supported data transformations, see [Data Transformation](./data-transformation.md).
