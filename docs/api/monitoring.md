# Monitoring API

> __Note__ - all responses here are correct if TSP started on
remote Flink cluster.

{% include basic-api-format.md %}


## Endpoints

Note: params with '*' suffix are required.

__Common path parameters for endpoints bellow__:

Name | Type | Description
---- | ---- | -----------
job_uuid* | String | Unique ID of job (`uuid` field in `streamJob`)  

__Common error codes__:  

Code | Description
---- | ----
4006 | Job with that name is not found




#### GET `job/{job_uuid}/status/`
Status and brief info about the job.  

- __Response__:  

Name | Type | Description
---- | ---- | -----------
name | String | UUID in query
duration | Long | job duration in milliseconds
status | Enum | One of Flink Job [statuses](https://ci.apache.org/projects/flink/flink-docs-stable/internals/job_scheduling.html)
start-time | Long | time, when job had been started
jid | String | Internal job uuid (hash)

__Example__:  

```json
{
  "response": {
      "duration": 22372,
      "name": "Albert_tests7",
      "state": "FINISHED",
      "start-time": 1538572801842,
      "jid": "ef2b95159960214b4f2753bafe91c7f2",
      "vertices": [/* internal metrics info */]
  },
  "messages": []
}
```


#### GET `job/{job_uuid}/statusAndMetrics/`
Status and metrics for the job    
  

__Response format__:  

Name | Type | Description
---- | ---- | ----------- 
metrics | Object | Key-value pairs with main [metrics of the job](./model/monitoring-metrics.md)
details | Object | Status data, same as in `status` end-point

__Response example__:  

```json
{
  "response": {
    "details": {
      "duration": 22372,
      "name": "Albert_tests7",
      "state": "FINISHED",
      "start-time": 1538572801842,
      "jid": "ef2b95159960214b4f2753bafe91c7f2",
      "vertices": [/* internal metrics info */] 
    },
    "metrics": {
      "numRecordsRead": "9197",
      "currentEventTs": "0",
      "numRecordsProcessed": "0"
    }
  },
  "messages": []
}
```


#### GET `job/{job_uuid}/stop/`
Stop job by uuid.  

__Responses__:  
- `{response: 1}` - job has stopped


#### GET `jobs/overview/`
List of all available jobs to cluster.   

__Response examples__:
- `{response: {name, jid}}`
- `{response: 0}` - no jobs


#### GET `jobs/{job_uuid}/exceptions`
Exception (if exists) for specific job.  

__Response__:  

Name | Type | Description
---- | ---- | -----------
root-exception | String | Fatal (for job) exception stacktrace
timestamp | Long | Timestamp (milliseconds in fractions) when occurred
truncated | Boolean | Does exception was too big so it truncated



#### GET "metainfo/getVersion/"
__Response__:  

Version in Semver format, for example `{response: "0.12.0"}`

