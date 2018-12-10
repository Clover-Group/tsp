#!/bin/bash

# Get last tag-version OR parse last untagged version, for example - 0.11.2-4-g1099d94-SNAPSHOT,
# and increase minor version - 0.11.3-4-g1099d94-SNAPSHOT
version=`git describe --tags --dirty="-SNAPSHOT" | \
         awk 'match($0, /\.([0-9]+)\-/){ \
                print substr($0, 2, RSTART-1) \
                int(substr($0, RSTART+1, RLENGTH-2))+1 \
                substr($0, RSTART+RLENGTH-1)\
              } \
              !match($0, /\.([0-9]+)\-/) {print substr($0, 2)}'`
defaultJarPath="./mainRunner/target/scala-2.12/TSP_v${version}.jar"

java ${TSP_JAVA_OPTS:--Xms1G -Xmx6G} \
     ${TSP_JAVA_TOOL_OPTS:--Dfile.encoding=UTF8} \
     -cp "${TSP_JAR:-$defaultJarPath}" \
         ru.itclover.tsp.http.Launcher flink-cluster-test "${TSP_JAR:-$defaultJarPath}" 
