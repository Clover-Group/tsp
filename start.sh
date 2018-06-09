#!/bin/bash


env JAVA_OPTS="${SM_JAVA_OPTS:--Xms2G -Xmx13G}" \
    JAVA_TOOL_OPTIONS="${SM_JAVA_TOOL_OPTS:--Dfile.encoding=UTF8}" \
    sbt "${SM_LAUNCHER:-http/runMain ru.itclover.streammachine.http.Launcher}" $SM_LAUNCHER_ARGS
