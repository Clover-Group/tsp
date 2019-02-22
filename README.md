Welcome to the TSP Engine !
===

TSP is a Time Series Patterns search engine. It is a backend system behind the [Clover Group Service platform](https://clover.global/en)

TSP is a distributed compute system implemented in Modern Scala. For more information, refer to [Documentation](https://clover-group.github.io/tsp/)

## Regression

Regression test allows to run a large number of predefined requests thru the REST API and easily and quickly validate TSP operation. 
Running regression doesn't require any specific knowledge of the TSP platform, beyond the ability to generate requests, which mimic a production system.

### Prerequesties: Python 3.5, Docker and REST-compatible requests

#### 1. Launch a docker runtime for the production environment:
``` bash
cd tsp/runtime
ln -s docker-compose.yml.prod docker-compose.yml
./start.sh
```
#### 2. Open your web browser and load the Apache Flink Dashboard:
``` bash 
localhost:8081
```
This should display several tabs. When you start the regression, your batches and requests should display here in **Running Jobs** or **Completed Jobs** tabs.


#### 3. Store requests in the regression folder:
``` bash
tsp/test/reqs
```

#### 4. Start regression
``` bash
cd tsp/test/regression
./tester.py
```

This packs requests from the **/tsp/test/reqs** folder into bathces and sends to the TSP backend thru the Docker runtime. All jobs should be visible in the Flink dashboard ( see 2 ). If any batch fails, the regression stops immediately and displays the error information
