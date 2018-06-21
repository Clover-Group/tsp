#!/bin/sh

set -e

if [ "$1" = 'start' ]; then
	./start-jar.sh

else
  exec "$@"
fi
