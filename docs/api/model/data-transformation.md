## Data transformation

TSP supports the following kinds of data transformation (i.e.
pre-processing before searching for patterns):
- NarrowDataUnfolding
- WideDataFilling

### NarrowDataUnfolding
This type unfolds data in "narrow" format (featuring only columns
for key and value) into "wide" one (featuring separate column for each
key), filling them with previous value if none specified at the given
timestamp.

#### Configuration parameters:
Name            |        Type       |                          Description
----------------|-------------------|-------------------------------------
key             |       String      | name of the column containing keys
value           |       String      | name of the column containing values
fieldsTimeoutsMs|Map\[String, Long\]| expiration timeouts for each key
defaultTimeout  |        Long       | default timeout for keys not specified in `fieldsTimeoutsMs`


#### Example

Raw stored data:


Time                | Key |  Value
--------------------|-----|-------
2018-11-01T12:00:00Z|key_1|      1
2018-11-01T12:00:00Z|key_3|      8
2018-11-01T12:00:05Z|key_2|     10
2018-11-01T12:00:10Z|key_1|      2
2018-11-01T12:00:15Z|key_3|      1
2018-11-01T12:00:20Z|key_4|      6
2018-11-01T12:00:25Z|key_2|     15

Configuration:
```json
{
  "key": "Key",
  "value": "Value",
  "fieldsTimeoutsMs": {"key_1": 15000, "key_2": 10000},
  "defaultTimeout": 5000,
}
```

Result:

Time                |key_1|key_2|key_3|key_4
--------------------|-----|-----|-----|-----
2018-11-01T12:00:00Z|    1|     |    8|
2018-11-01T12:00:05Z|    1|   10|    8|
2018-11-01T12:00:10Z|    2|   10|     |
2018-11-01T12:00:15Z|    2|   10|    1|
2018-11-01T12:00:20Z|    2|     |    1|    6
2018-11-01T12:00:25Z|    2|   15|     |    6

(Note that the filling occurs only until specified timeout expires;
`key_3` and `key_4` use default timeout of 5000 milliseconds.)

### WideDataFilling
This type expects "wide" data as its input, doing only the filling
(which is the same as in NarrowDataUnfolding; it is especially useful
for InfluxDB source, which stores values separately by column, but
cannot fill with previous values using expiration by time).

#### Configuration
It accepts only `fieldsTimeoutsMs` and `defaultTimeout` fields, with
the same semantics as in NarrowDataUnfolding (see above).