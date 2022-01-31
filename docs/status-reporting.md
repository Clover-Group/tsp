## Status reporting

TSP can use Kafka for reporting job statuses upon 
submitting and finishing execution (with success or error). 

The messages are sent with following schema: 

```json
    {
      "uuid": "job uuid from HTTP query",
      "timestamp": "2022-01-31T05:18:23.409067Z",
      "status": "SUBMITTED",
      "flinkStatus": "RUNNING",
      "text": "additional info message text"
    }
```