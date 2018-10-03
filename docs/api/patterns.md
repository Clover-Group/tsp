# Patterns search API

{% include basic-api-format.md %}


## Endpoints

Note: params with '*' suffix are required.

### POST `streamJob/from-{source}/to-{sink}/`
Main method to run patterns search job.

__Path parameters__:

Name | Type | Description
---- | ---- | -----------
source* | Enum | type of source to read data from, possible values: `jdbc`, `influxdb`
sink* | Enum | type of sink to write incidents to, possible values: `jdbc`, `influxdb`, `kafka` (beta)

__URL parameters__:

Name | Type | Description | Default
---- | ---- | ----------- | -------
run_async | Boolean | do send preserve connection (and send back all errors) during whole life of request? | `false`

__Body parameters__:

Name | Type | Description
---- | ---- | -----------
uuid* | String | Unique ID of job for further use in monitoring API
source* | [Source](./model/sources.md) | Configs to specific type of source provided in path param `source`
sink* | [Sink](./model/sinks.md) | Configs to specific type of sink provided in path param `sink`
patterns* | List[[Pattern](./model/pattern.md)] | Patterns source-code to parse and run on source data

