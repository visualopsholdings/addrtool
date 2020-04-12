#!/bin/bash
#
# backup DB

if [ "$#" -ne 3 ]; then
	echo "usage $0 dbname newcoll filename"
	exit 1
fi

DB=$1
NEWCOLL=$2
FILENAME=$3

rm -rf dump
mongodump -d $DB -c $NEWCOLL
ERROR=$?
if [ $ERROR -ne 0 ]
then
	echo "error:" $ERROR
	exit 1
fi

tar czf $FILENAME dump
echo "$NEWCOLL backed up to $FILENAME"
rm -rf dump
