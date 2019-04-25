# This script checks log files for errors of specified type

DIR=./logs/

grep -rin "xception" $DIR
