# Monitoring API

__Note__ - all responses here are correct if TSP started on
remote Flink cluster.

API respond with [general response fromat](./index.md).


### Jos endpoints

Error responses same as for `streamJob`-s

#### GET "job/:job_uuid/statusAndMetrics/"

- Response formats:
    - `numRecordsReadNum` - records read from source
    - `currentEventTs` - records handled by patterns
    - `numRecordsProcessed` - records sent to sink (approx.)
```
"details": <same as job/:job_uuid/status/>,
"metrics": {
  "numRecordsRead": <String>,
  "currentEventTs": <String>,
  "numRecordsProcessed": <String>
}
```

#### GET "job/:job_uuid/status/"

- Response formats:
`{"duration": <Long millis>, "name": <Task uuid>,
"numProcessedRecords": <Long>, "state": "RUNNING",
"start-time":<Unix TS in millis Long>,"jid":<Internal ID, String>,
"vertices":<Not matter>}`


#### GET "job/:job_uuid/stop/"

Responses:
- `{response: 1}` - job has stopped
- `{response: 0}` - job not found


#### GET "jobs/overview/"
Responses:
- `{response: {name, jid}}`
- `{response: 0}` - no jobs


#### GET "jobs/:job_uuid/exceptions"
`{"root-exception": <String>,"timestamp": <Long>, "truncated": <Bool>}`
- `truncated` shows that exception is truncated when it's too big


### Other endpoints

#### GET "metainfo/getVersion/"
Response -  `{response: "x.y.z"}`

