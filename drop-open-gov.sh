#!/bin/bash
#
# drop all created tables.

if [ "$#" -ne 1 ]; then
	echo "usage $0 dbname"
	exit 1
fi

DB=$1

mongo <<EOF
use $DB
db.open_gov.drop()
EOF
