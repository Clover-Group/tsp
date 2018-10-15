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


__Response__: 
- Success example: `{'response': {"execTimeSec": 100}}`
- Failure: Generic error response


__Error codes__:

Code | Description
---- | -----------
*4001* | *Invalid patterns source code*
{% include generic-api-errors.md %}

### POST `patterns/validate`
Endpoint for validation of the patterns syntax without actually being run.

__Body parameters__:

Name | Type | Description
---- | ---- | -----------
patterns* | List[[Pattern](./model/pattern.md)] | Patterns source-code to parse

__Response__:

On success returns a list of objects which have the following structure:

Name | Type | Description
---- | ---- | -----------
pattern | Pattern | A pattern which was checked
success | Boolean | True if the pattern syntax is correct, false otherwise
context | String | Internal representation of a pattern builder if the pattern is correct, error description otherwise

__Error codes__:

Code | Description
---- | -----------
*4001* | *Invalid patterns source code*
{% include generic-api-errors.md %}