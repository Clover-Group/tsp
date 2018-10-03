
### Basic API response format

- __Success__: `{'response': ...}`  
__Example__:
`{'response': {"execTimeSec": 100}}`

- __Failure__: `{'errorCode': Int, 'message': String, 'errors': List[String], ...}`  
__Example__:
```
{
    "errorCode": 5001,
    "message": "Job execution failure",
    "errors": ["Uncaught error during connection to ..."]
}
```
