Documentation
=============

### Overview
__TSP__ (Time Series Patterns) - analytical backend for searching
patterns in high-volume time-series data.


__Key Properties:__
- Agile - flexible DSL for expressing unique patterns
- Fast - optimized stream-based processing engine on top of Apache Flink


__Example patterns:__
- Invalid use of engine - "5 seconds average engine speed greater than 5000 for 10 min then maximum oil pump for 20 sec is bigger than zero":
```
avg(engineSpeed, 5 sec) > 5000 for 10 min 
    andThen 
max(oilPump, 20 sec) > 0
```

- Sessionization by user activity - "Time between any user actions is less than 10 min":
<br>`anyOf(click, move, ...) for 10 minutes > 0 times`

- Behavioral analytics - "User spent too little time on advertisement page":
<br>`activePage = "AdPage" for lessThen 0 sec`

- [More about writing patterns](./writing-patterns.md)


__Deployment options:__
<br>It can be used as the service and library:
- As a service it:
    - Takes as input: source and sink type in URL (JDBC, InfluxDB or Kafka topic
    for now), then configuration and patterns to search in the body
    ([endpoints details](./api/patterns.md))
    - Write incidents (occurrences of the pattern) in the sink as they're found, with neighbours united together
    - Provide monitoring [end-points](./api/monitoring.md)


### Guides
- [Getting started](./getting-started.md)
- [Writing patterns](./writing-patterns.md)
- [Debugging guidelines](./debugging-guidelines.md)
- [Deployment](./deployment.md)
- [Contribution guide](./contribution-guide.md)
    - [Coding guidelines](./coding-guidelines.md)

### API Documentation
- [Basic format](./api/index.md)
- [Patters search](./api/patterns.md)
- [Monitoring](./api/monitoring.md)

