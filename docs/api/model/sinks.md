# Sink types

> Note: {% include types-note.md %}

Generic parameters:
- `rowSchema` (JSON object) is the row schema for sink (keys are incidents fields, values are table columns/JSON object keys)

### JDBC sink
- `jdbcUrl`, `driverName`, `userName`, `password` - the same as in JDBC source
- `tableName` (string) is the name of an SQL table to store incidents.
- `batchInterval` (integer) is the size of a single batch to write (in rows)

### Kafka sink
- `broker` and `topic` - the same of Kafka sink (note: for historical reasons, it is called `broker` in singular in sink)
- `serializer` (string, default `"json"`) is the serializer for incident messages. 