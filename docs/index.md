Documentation
=============

### Overview
__TSP__ (Time Series Patterns) - the service and library for searching
patterns in high-volume time-series data.
- As a service it:
    - Takes as input: source and sink type in URL (JDBC or InfluxDB
    for now), they configuration and patterns to search in the body
    ([endpoints details](./api/patterns.md))
    - Write events in sink as they're found
- As a library it provides functions for transforming each input
event and some previous state into some result and new state
(see [architectural overview](./architectural-overview.md)).


__Key Properties:__
- Agile - flexible DSL for expressing unique patterns
- Fast - optimized stream-based processing engine on top of Apache Flink


### Guides
- [Architectural overview](./architectural-overview.md)
- [Writing patterns](./writing-patterns.md) TODO
- [Debugging guidelines](./debugging-guidelines.md)
- [Installation](./installation.md)
- [Contribution guide](./contribution-guide.md)
    - [Coding guidelines](./coding-guidelines.md)

### API Documentation
- [Basic format](./api/index.md)
- [Patters search](./api/patterns.md)
- [Monitoring](./api/monitoring.md)
