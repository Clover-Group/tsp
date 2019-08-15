#!/bin/bash
set -m

sh -c java -agentpath:/opt/docker/conf/libyjpaagent.so=port=10001,listen=all \
  "${TSP_JAVA_OPTS:--Xms1G -Xmx6G}" -jar /opt/tsp.jar "${EXECUTION_TYPE}"