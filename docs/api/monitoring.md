# Monitoring API

__Note__ - all responses here are correct if TSP started on
remote Flink cluster.

API respond with [general response fromat](./index.md).


### Endpoints

Error responses same as for `streamJob`-s

#### 1. GET "job/:job_uuid/status/"

- Response formats:
`{"duration": <Long millis>, "name": <Task uuid>,
"numProcessedRecords": <Long>, "state": "RUNNING",
"start-time":<Unix TS in millis Long>,"jid":<Internal ID, String>,
"vertices":<Not matter>}`


#### 2. GET "job/:job_uuid/stop/"

Responses:
- `{response: 1}` - job has stopped
- `{response: 0}` - job not found


#### 3. GET "jobs/overview/"
Responses:
- `{response: {name, jid}}`
- `{response: 0}` - no jobs


#### 4. GET "jobs/:job_uuid/exceptions"
`{"root-exception": <String>,"timestamp": <Long>, "truncated": <Bool>}`
- `truncated` shows that exception is truncated when it's too big

#### 5. GET "metainfo/getVersion/"
Response -  `{response: "x.y.z"}`

