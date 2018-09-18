#!/bin/bash


env JAVA_OPTS="${TSP_JAVA_OPTS:--Xms2G -Xmx9G}" \
    JAVA_TOOL_OPTIONS="${TSP_JAVA_TOOL_OPTS:--Dfile.encoding=UTF8}" \
    sbt "project integrationPerformance" test
