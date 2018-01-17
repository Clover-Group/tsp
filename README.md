StreamMachine
=============

A Flink application project using Scala and SBT.

To run and test your application use SBT invoke: 'sbt run'

In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.


API endpoints
-------------

### Response format
#### Success:
`{'response': ...}`
#### Failure:
`{'errorCode': Int, 'message': String, 'errors': List[String], ...}`


### Methods

#### 1. POST "streaming" / "find-patterns" / "wide-dense-table" /

__GET Params:__
- `` - url-escaped phase code to compile

__Request body:__

```
{
    "source": {
        "jdbcUrl": String, // e.g.
        "query": String,
        "driverName": String,
        "datetimeColname": String,
        "partitionColnames": Array[String],
        "userName": Option[String],
        "password": Option[String]
    },
    "sink": {
        "jdbcUrl": String,
        "sinkTable": String,
        "sinkColumnsNames": Array[String],
        "driverName": String,
        "batchInterval": Option[Int]
    },
    "patternsCodes": Array[String]
}
```

__Example request body__

```
{
    "source": {
        "jdbcUrl": "jdbc:clickhouse://localhost:8123/test",
        "query": "select timestamp, Wagon_id, speed from series765_data limit 0, 50000",
        "driverName": "ru.yandex.clickhouse.ClickHouseDriver",
        "datetimeColname": "date_time",
        "partitionColnames": ["Wagon_id"],
        "userName": "test",
        "password": "test"
    },
    "sink": {
        "jdbcUrl": "jdbc:clickhouse://localhost:8123/renamedTest",
        "sinkTable": "series765_data_sink_test_speed",
        "sinkColumnsNames": ["is_rule_success"],
        "driverName": "ru.yandex.clickhouse.ClickHouseDriver",
        "batchInterval": 5000
    },
    "patternsCodes": ["Assert[Row](event => event.getField(3).asInstanceOf[Float].toDouble > 250)"]
}
```
