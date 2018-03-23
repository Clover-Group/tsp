#!/bin/sh

set -e

if [ "$1" = 'start' ]; then
	env JAVA_OPTS="-Xms2G -Xmx8G" JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF8' sbt "http/runMain ru.itclover.streammachine.http.Launcher"

else
  exec "$@"
fi
