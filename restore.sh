#!/bin/bash
#
# restore DB

if [ "$#" -ne 1 ]; then
	echo "usage $0 dbname"
	exit 1
fi

DB=$1

mongorestore -d $DB --drop dump/$DB
