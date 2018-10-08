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
2. Start the image - `docker run -p 8664:8080 -e EXECUTION_TYPE=<etype> tsp:<tag>`,
where `<etype>` is currently `flink-local` or `flink-cluster` (it specifies Flink
running mode), and `8664` is the port of the host machine which will be mapped to the
container's port `8080` (used for the main HTTP interface).