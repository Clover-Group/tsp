#!/bin/sh

set -e

if [ "$1" = 'start' ]; then
	./start.sh

else
  exec "$@"
fi
