#!/bin/bash
#
# backup all DBs

if [ "$#" -ne 3 ]; then
	echo "usage $0 dbname newcoll fileprifix"
	exit 1
fi

./backup1.sh $1 $2 $3.tgz
./backup1.sh $1 "$2"_tuples "$3"_tuples.tgz
