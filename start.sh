#!/bin/bash


env JAVA_OPTS=${SM_JAVA_OPTS:-"-Xms2G -Xmx12G"} \
    JAVA_TOOL_OPTIONS=${SM_TOOL_OPTS:-'-Dfile.encoding=UTF8'} \
    sbt "http/runMain ru.itclover.streammachine.http.TestLauncher"
