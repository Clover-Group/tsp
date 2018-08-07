StreamMachine
=============

A Flink application project using Scala and SBT.

To run and test your application use SBT invoke: 'sbt run'

In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.

Installation
------------

1. Install sbt 1.0+
2. Run `./compile-jar.sh`
2. Run `./start-jar.sh`

Writing patterns
----------------

[Documentation](PatternsDoc.md)


API endpoints
-------------

### Response format
#### Success:
- Generic `{'response': ...}`
- Successful synchronous job `{'response': {"execTimeSec": Long}}`
#### Failure:
`{'errorCode': Int, 'message': String, 'errors': List[String], ...}`


### Types used in requests
Translated to json directly

1. [@InputConf](flink/src/main/scala/ru/itclover/streammachine/io/input/InputConf.scala)

2. [@OutputConf](flink/src/main/scala/ru/itclover/streammachine/io/output/OutputConf.scala)

3. [@Pattern](flink/src/main/scala/ru/itclover/streammachine/io/input/RawPattern.scala)

### Methods

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
        "query": "select date_time, loco_id, CurrentBattery from TE116U_062_SM_TEST limit 0, 50000",
        "driverName": "ru.yandex.clickhouse.ClickHouseDriver",
        "datetimeColname": "date_time",
        "partitionColnames": ["loco_id"],
        "eventsMaxGapMs": 60000,
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

### Monitoring

Error responses same as for `streamJob`-s

#### 1. Get "job/:job_uuid/status/"

- Response formats:
    - Running Job
`{"duration": <Long millis>, "name": <Task uuid>,
"numProcessedRecords": <Long>, "state": "RUNNING",
"start-time":<Unix TS in millis Long>,"jid":<Internal ID, String>,
"vertices":<Not matter>}`
    - Finished or non-existent job: `{response: 0}`


#### 2. Get "job/:job_uuid/stop/"

Responses:
- `{response: 1}` - job has stopped
- `{response: 0}` - job not found


#### 3. Get "jobs/overview/"
Responses:
- `{response: {name, jid}}`
- `{response: 0}` - no jobs

#### 4. Get "metainfo/getVersion/"
Response -  `{response: "x.y.z"}`

