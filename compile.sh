#!/bin/bash

env JAVA_TOOL_OPTIONS='-Dfile.encoding=UTF8' sbt "http/compile"
