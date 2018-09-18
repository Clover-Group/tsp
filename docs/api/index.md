# General response format

#### Success:
- Generic `{'response': ...}`
- Successful synchronous job `{'response': {"execTimeSec": Long}}`

#### Failure:
`{'errorCode': Int, 'message': String, 'errors': List[String], ...}`


### Documentation
- [Patterns jobs](./patterns.md)
- [Monitoring](./monitoring.md)
