StreamMachine
=============

A Flink application project using Scala and SBT.

To run and test your application use SBT invoke: 'sbt run'

In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.

Installation
------------

1. Install sbt 1.0+
2. run `./start.sh` (Linux)

Writing patterns
----------------

[Documentation](PatternsDoc.md)


API endpoints
-------------

### Response format
#### Success:
`{'response': ...}`
#### Failure:
`{'errorCode': Int, 'message': String, 'errors': List[String], ...}`


### Types used in requests
Translated to json directly

1. [@InputConf](flink/src/main/scala/ru/itclover/streammachine/io/input/InputConf.scala)

2. [@OutputConf](flink/src/main/scala/ru/itclover/streammachine/io/output/OutputConf.scala)

### Methods

#### 1. POST "streaming" / "find-patterns" / "wide-dense-table" /

__GET Params:__

__Request body:__

```
{
    "source": @InputConf
    "sink": @OutputConf
    "patternsIdsAndCodes": Map[String, String]
}
```

__Example request body__

```
{
    "source": {
        "jdbcUrl": "jdbc:clickhouse://localhost:8123/default",
        "query": "select date_time, loco_id, CurrentBattery from TE116U_062_SM_TEST limit 0, 50000",
        "driverName": "ru.yandex.clickhouse.ClickHouseDriver",
        "datetimeColname": "date_time",
        "partitionColnames": ["loco_id"],
        "userName": "test",
        "password": "test"
    },
    "sink": {
        "jdbcUrl": "jdbc:clickhouse://localhost:8123/renamedTest",
        "sinkSchema": {"tableName": "series765_data_res", "fromTimeField": "from", "fromTimeMillisField": "from_millis",
            "toTimeField": "to", "toTimeMillisField": "to_millis", "patternIdField": "rule_id", "forwardedFields": ["loco_id"]},
        "driverName": "ru.yandex.clickhouse.ClickHouseDriver",
        "batchInterval": 5000
    },
    "patternsIdsAndCodes": {"1": "Assert[Row](event => event.getField(2).asInstanceOf[Float].toDouble > 10)"}
}
```

#### 2. POST "streaming" / "find-patterns" / "narrow-table" /

__GET Params:__

__Request body:__

```
{
    "source": @InputConf.JDBCNarrowInputConf
    "sink": @OutputConf
    "patternsIdsAndCodes": Map[String, String]
}
```

__Example request body__
```
{
    "source": {
        "jdbcConf": {
            "jdbcUrl": "jdbc:clickhouse://localhost:8123/default",
            "query": "select date_time, loco_id, CurrentBattery from TE116U_062_SM_TEST limit 0, 50000",
            "driverName": "ru.yandex.clickhouse.ClickHouseDriver",
            "datetimeColname": "date_time",
            "partitionColnames": ["loco_id"],
            "userName": "test",
            "password": "test",
            "eventsMaxGapMs": 60000
        },
        keyColname: "sensor_id",
        valColname: "value_float",
        fieldsTimeoutsMs: {"speed": 1000, "pump": 10000}
    },
    "sink": {
        "jdbcUrl": "jdbc:clickhouse://localhost:8123/renamedTest",
        "sinkSchema": {"tableName": "series765_data_res", "fromTimeField": "from", "fromTimeMillisField": "from_millis",
            "toTimeField": "to", "toTimeMillisField": "to_millis", "patternIdField": "rule_id", "forwardedFields": ["loco_id"]},
        "driverName": "ru.yandex.clickhouse.ClickHouseDriver",
        "batchInterval": 5000
    },
    "patternsIdsAndCodes": {"1": "Assert[Row](event => event.getField(2).asInstanceOf[Float].toDouble > 10)"}
}
```