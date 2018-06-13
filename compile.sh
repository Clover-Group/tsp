#!/bin/bash


env JAVA_TOOL_OPTIONS="${SM_JAVA_TOOL_OPTS:--Dfile.encoding=UTF8}" sbt "http/compile"
