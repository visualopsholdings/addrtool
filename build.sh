#!/bin/bash
#

if [ "$#" -ne 2 ]; then
	echo "usage $0 dbname newcoll"
	exit 1
fi

DB=$1
NEWCOLL=$2

cargo run $DB $NEWCOLL 1 act --drop
cargo run $DB $NEWCOLL 2 nsw
cargo run $DB $NEWCOLL 3 nt
cargo run $DB $NEWCOLL 4 ot
cargo run $DB $NEWCOLL 5 qld
cargo run $DB $NEWCOLL 6 sa
cargo run $DB $NEWCOLL 7 tas
cargo run $DB $NEWCOLL 8 vic
cargo run $DB $NEWCOLL 9 wa
