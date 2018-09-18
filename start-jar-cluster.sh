#!/bin/bash

version=`cat VERSION`
defaultJarPath="./mainRunner/target/scala-2.11/TSP_v${version}.jar"

java ${TSP_JAVA_OPTS:--Xms1G -Xmx6G} \
     ${TSP_JAVA_TOOL_OPTS:--Dfile.encoding=UTF8} \
     -cp "${TSP_JAR:-$defaultJarPath}" \
         ${TSP_MAIN_CLASS:-ru.itclover.tsp.http.Launcher} flink-cluster $TSP_LAUNCHER_ARGS
