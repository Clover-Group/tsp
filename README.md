StreamMachine
=============

A Flink application project using Scala and SBT.

To run and test your application use SBT invoke: 'sbt run'

In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.

Installation
------------

1. Install sbt 1.0+
2. run `./start.sh` (Linux)


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

__Request body:__

```
{
    "source": {
        "jdbcUrl": String,
        "query": String,
        "driverName": String,
        "datetimeColname": String,
        "partitionColnames": Array[String],
        "userName": Option[String],
        "password": Option[String]
    },
    "sink": {
        "jdbcUrl": String,
        "sinkSchema": (tableName: String, fromTimeField: Symbol, fromTimeMillisField: Symbol,
                       toTimeField: Symbol, toTimeMillisField: Symbol,
                       patternIdField: Symbol, forwardedFields: Array[Symbol])
        "driverName": String,
        "batchInterval": Option[Int]
    },
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
