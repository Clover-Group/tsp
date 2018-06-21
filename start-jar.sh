#!/bin/bash

version=`cat VERSION`
defaultJarPath="./mainRunner/target/scala-2.11/StreamMachine_v${version}.jar"

env JAVA_OPTS="${SM_JAVA_OPTS:--Xms2G -Xmx4G}" \
    JAVA_TOOL_OPTIONS="${SM_JAVA_TOOL_OPTS:--Dfile.encoding=UTF8}" \
    java -cp "${SM_JAR:-$defaultJarPath}" \
             "${SM_MAIN_CLASS:-ru.itclover.streammachine.http.Launcher}" $SM_LAUNCHER_ARGS
