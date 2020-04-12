# create all the indexes.

if [ "$#" -ne 2 ]; then
	echo "usage $0 dbname newcoll"
	exit 1
fi

DB=$1
NEWCOLL=$2

mongo <<EOF
use $DB
db.runCommand(
	{ createIndexes: "$NEWCOLL", indexes: [ 
  		{ key: { "ADDRESS_DETAIL_PID": 1 }, name: "ADDRESS_DETAIL_PID" }, 
  		{ key: { "STATE": 1 }, name: "STATE" }, 
  		{ key: { "LOCALITY": 1 }, name: "LOCALITY" }, 
  		{ key: { "POSTCODE": 1 }, name: "POSTCODE" }
	]}
)
EOF
