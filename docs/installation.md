# Installation

__Via Intellij:__
- In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.


__Via JAR:__
1. Install sbt 1.0+
2. Compile - `./compile-jar.sh`
3. Start - `./start-jar.sh`

__Via Sbt:__
1. Run `./start-sbt.sh`

__Via Docker:__
1. Pull the image - `docker pull clovergrp/tsp:<tag>`, where `<tag>` is the desired 
version number without the `v` prefix (e.g. `0.12.3`).
The Docker Hub page for TSP can be found
 [here](https://hub.docker.com/r/clovergrp/tsp/).
2. Start the image - `docker run -p 8664:8080 -e EXECUTION_TYPE=<etype> tsp:<tag>`,
where `<etype>` is currently `flink-local` or `flink-cluster` (it specifies Flink
running mode), and `8664` is the port of the host machine which will be mapped to the
container's port `8080` (used for the main HTTP interface).



## Testing and profiling setup 
#### Setup flink:
1. Install Flink 1.6.1
2. Run local cluster `cd flink && ./bin/start-cluster.sh`
3. Check web ui at http://localhost:8081/#/overview (should be 1 slot available)
1. Clone repo
#### Meanwhile, setup Tsp:
1. Clone repo  
  
Simple local deployment, without Flink Web UI: `sbt run`  
  
OR, compile and run via Jar for remote environment, with Flink Web UI:
1. Compile `./compile-jar.sh`
2. Export jar path: `export TSP_JAR=<PATH_TO_JAR>`
3. Run `./start-jar-cluster-test.sh` 
4. Attach to the process via profiler

#### Make test requests to API via HTTP
[Details](./api/index.md) about requests to API.
