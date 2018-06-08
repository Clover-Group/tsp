#!/bin/bash

env JAVA_OPTS="-Xms2G -Xmx4G -XX:+UseParallelOldGC -Xloggc:gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+PrintFlagsFinal -XX:+UnlockDiagnosticVMOptions"  sbt "http/runMain ru.itclover.streammachine.http.SMLauncher"
