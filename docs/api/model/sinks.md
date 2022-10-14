# Sink types

> Note: {% include types-note.md %}

Generic parameters:
- `rowSchema` (JSON object) is the row schema for sink (keys are incidents fields, values are table columns/JSON object keys). All fields are required and have a string type unless other specified.
  * `fromTsField` - the column name for the starting timestamp of an incident 
  * `toTsField` - the column name for the ending timestamp of an incident
  * `unitIdField` - the column name for the unit ID of an event (the value is taken from source)
  * `appIdFieldVal` (string and integer) - the column name and value for the incident type (for example, to distinguish different incident categories).
  * `patternIdField`

#### Example
```json
{
  "toTsField": "to_ts",
  "fromTsField": "from_ts",
  "unitIdField": "engine_id",
  "appIdFieldVal": ["rule_type", 1],
  "patternIdField": "rule_id",
  "subunitIdField": "physical_id",
  "incidentIdField": "uuid"
}
```

### JDBC sink
- `jdbcUrl`, `driverName`, `userName`, `password` - the same as in JDBC source
- `tableName` (string) is the name of an SQL table to store incidents.
- `batchInterval` (optional, integer) is the size of a single batch to write (in rows)

#### Example
(no `rowSchema` is given, see above)
```json
{
  "jdbcUrl": "jdbc:clickhouse://default:@127.0.0.1:8123/mydb",
  "tableName": "engine_events",
  "driverName": "com.clickhouse.jdbc.ClickHouseDriver"
}
```

### Kafka sink
- `broker` and `topic` - the same of Kafka sink (note: for historical reasons, it is called `broker` in singular in sink)
- `serializer` (string, default `"json"`) is the serializer for incident messages. 

#### Example
(no `rowSchema` is given, see above)
```json
{
  "broker": "10.83.0.3:9092",
  "topic": "engine_events"
}
```