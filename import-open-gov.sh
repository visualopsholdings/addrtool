#!/bin/bash
#
# change the path below to where you unzipped your downloaded data.

if [ "$#" -ne 1 ]; then
	echo "usage $0 dbname"
	exit 1
fi

DB=$1

pushd PATH_TO_UNZIPPED_DATA
#pushd ~/Downloads
mongoimport --type csv -d $DB -c open_gov --headerline --drop Address_Point.csv
popd
