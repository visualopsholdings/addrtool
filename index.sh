#!/bin/bash
#
# create all the indexes.

if [ "$#" -ne 2 ]; then
	echo "usage $0 dbname newcoll"
	exit 1
fi

DB=$1
NEWCOLL=$2
NEWTUPLECOLL=$2_tuples

mongo <<EOF
use $DB
db.runCommand(
	{ createIndexes: "$NEWCOLL", indexes: [ 
  		{ key: { "EID": 1 }, name: "EID" }, 
  		{ key: { "STATE": 1 }, name: "STATE" }, 
  		{ key: { "ZIP": 1 }, name: "ZIP" }
	]}
)
db.runCommand(
  { createIndexes: "$NEWTUPLECOLL", indexes: [ 
  	{ key: { "STATE": 1 }, name: "STATE" },
  	{ key: { "LOCALITY": 1 }, name: "LOCALITY" },
  	{ key: { "ZIP": 1 }, name: "ZIP" } 
  ] }
)
EOF
