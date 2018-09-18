# Patterns search API
API respond with [general response fromat](./index.md).

### Types used in requests
Translated to json directly

1. [@InputConf](flink/src/main/scala/ru/itclover/streammachine/io/input/InputConf.scala)

2. [@OutputConf](flink/src/main/scala/ru/itclover/streammachine/io/output/OutputConf.scala)

3. [@Pattern](flink/src/main/scala/ru/itclover/streammachine/io/input/RawPattern.scala)

### Endpoints

#### 1. POST "streamJob/from-jdbc/to-jdbc/"

__GET Params:__

__Request body:__

```
{
    "uuid": String,
    "source": @InputConf,
    "sink": @OutputConf,
    "patternsIdsAndCodes": [@Pattern]
}
```

__Example request body__

```
{
    "uuid": "test",
    "source": {
        "jdbcUrl": "jdbc:clickhouse://localhost:8123/default",
        "query": "select date_time, id_tag, CurrentBattery from TEST_TABLE limit 0, 50000",
        "driverName": "ru.yandex.clickhouse.ClickHouseDriver",
        "datetimeColname": "date_time",
        "partitionColnames": ["id_tag"],
        "eventsMaxGapMs": 60000,
        "userName": "test",
        "password": "test"
    },
    "sink": {
        "jdbcUrl": "jdbc:clickhouse://localhost:8123/MwmTest",
        "sinkSchema": {"tableName": "series765_data_res", "fromTimeField": "from", "fromTimeMillisField": "from_millis",
            "toTimeField": "to", "toTimeMillisField": "to_millis", "patternIdField": "rule_id", "forwardedFields": ["id_tag"]},
        "driverName": "ru.yandex.clickhouse.ClickHouseDriver",
        "batchInterval": 5000
    },
    "patternsIdsAndCodes": {"1": "CurrentBattery > 10"}
}
```

__Example response:__
```
{
    "response": {
        "execTimeSec": 1
    },
    "messages": []
}
```
