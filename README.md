StreamMachine
=============

A Flink application project using Scala and SBT.

To run and test your application use SBT invoke: 'sbt run'

In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.


API endpoints
-------------

### Формат ответа
#### Успех:
`{'response': ...}`
#### Неудачно:
`{'errorCode': Int, 'message': String, 'errors': List[String], ...}`


### Методы

#### 1. POST "streaming" / "find-patterns" / "wide-dense-table" /
__GET Params:__
- `phaseCode: String` - url-escaped phase code to compile
__Request body:__
```json
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
        "sinkTable": String,
        "sinkColumnsNames": Array[String],
        "driverName": String,
        "batchInterval": Option[Int]
    }
}
```


