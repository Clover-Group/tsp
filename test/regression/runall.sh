# This script runs all predefined tests from different folders 
# Use for sign off, debug and quick build validation
# Boris V.Kuznetsov
# Apr 25 2019

CPPKDIR=./cppk
LOCODIR=./loco

for i in $LOCODIR/* ; do ./tester_loco.py $i;done
for i in $CPPKDIR/* ; do ./tester_cppk.py $i;done
