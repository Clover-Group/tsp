# Debugging guidelines

### Known bugs

- __No results in sink:__ <br>
In version _bellow 0.9_ - may be if the last successful segment in
the source data was not closed by ending nulls, or failure result.
Or just no single pattern was found.


- `JobManager responsible for f8cb9b9e4 lost the leadership.` -
this error caused by our Flink deployment style, try to run job again,
it is not persistent.