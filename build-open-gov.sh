#!/bin/bash
#

if [ "$#" -ne 2 ]; then
	echo "usage $0 dbname newcoll"
	exit 1
fi

DB=$1
NEWCOLL=$2

cargo run $DB $NEWCOLL --opengov
