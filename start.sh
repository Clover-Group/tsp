#!/bin/bash

env JAVA_OPTS="-Xms2G -Xmx14G" JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF8' sbt "http/runMain ru.itclover.streammachine.http.Launcher"
