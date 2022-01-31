# Getting started

## Using via Docker

### Images

Docker images are available at `clovergrp/tsp`. The tags correspond to the version numbers, e.g. `0.15.29` or `17.0.1`.

_Note: Don't use the `latest` tag since it is inconsistent and may point to different branches at different times._

### Configuration variables

TSP supports following environment variables:
- `EXECUTION_TYPE` - may be either `flink-local` (to run TSP in local Flink mode, no need for an external cluster) or `flink-cluster` (run with external Flink).
- `FLINK_JOBMGR_HOST` and `FLINK_JOBMGR_PORT` (relevant only in cluster mode) - location of external Flink job manager (IP/hostname and port respectively).
- `FLINK_MONITORING_HOST` and `FLINK_MONITORING_PORT` - the same for monitoring API (usually the same as job manager host/port).
- `JOB_REPORTING_ENABLED` - used to enable job status reporting (to Kafka, off by default). A Boolean switch (available values are 1/0, on/off, yes/no or true/false). (*Note: If the reporting is off, status messages will appear in the container logs.*)
- `JOB_REPORTING_BROKER` and `JOB_REPORTING_TOPIC` (relevant if the job reporting is on) - the Kafka broker location (*host:port*) and topic name used for reporting. 

### Versioning policy
Since version 17.0.0, TSP uses SemVer "major.minor.patch" version scheme (e.g. 17.0.1). In particular:

- **major** version change (e.g. 17 &#8594; 18) indicates that the API may be broken, and the endpoints/query body schemes might change;
- **minor** version change (e.g. 17.0 &#8594; 17.1) indicates new features being added *without* breaking API compatibility (e.g. introducing new *optional* parameters, adding new data source formats etc.)
- **patch** version change (e.g. 17.0.1 &#8594; 17.0.2) indicates only bugfixes, without adding new functionality.

Prior to 17.0.0, the major version was zero (e.g. 0.16.34), the minor version change (0.15 &#8594; 0.16) indicated compatibility break, and both feature changes and bugfixes were indicated on the "patch" level. Since then, the leading zero was dropped and the planned `0.17.0` became `17.0.0`. 