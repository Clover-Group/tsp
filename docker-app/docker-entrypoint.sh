#!/bin/sh

set -e

if [ "$1" = 'start' ]; then
	env JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF8' sbt "http/runMain ru.itclover.streammachine.http.DockerWebServer"

else
  exec "$@"
fi
