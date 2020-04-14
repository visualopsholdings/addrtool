#!/bin/bash
#
# backup DB

if [ "$#" -ne 3 ]; then
	echo "usage $0 dbname coll filename"
	exit 1
fi

DB=$1
COLL=$2
FILENAME=$3

rm -rf dump
mongodump -d $DB -c $COLL
ERROR=$?
if [ $ERROR -ne 0 ]
then
	echo "error:" $ERROR
	exit 1
fi

tar czf $FILENAME dump
echo "$COLL backed up to $FILENAME"
