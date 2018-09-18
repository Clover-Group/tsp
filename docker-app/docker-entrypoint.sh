#!/bin/sh

set -e

if [ "$1" = 'start-local' ]
then
	./start-jar-local.sh
elif [ "$1" = 'start-cluster' ]
then
    ./start-jar-cluster.sh
else
    exec "$@"
fi
