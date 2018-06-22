#!/bin/bash

version=`cat VERSION`
defaultJarPath="./mainRunner/target/scala-2.11/StreamMachine_v${version}.jar"

java ${SM_JAVA_OPTS:--Xms1G -Xmx2G} \
     ${SM_JAVA_TOOL_OPTS:--Dfile.encoding=UTF8} \
     -cp "${SM_JAR:-$defaultJarPath}" \
         ${SM_MAIN_CLASS:-ru.itclover.streammachine.http.Launcher} $SM_LAUNCHER_ARGS
