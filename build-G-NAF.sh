#!/bin/bash
#

if [ "$#" -ne 2 ]; then
	echo "usage $0 dbname newcoll"
	exit 1
fi

DB=$1
NEWCOLL=$2

cargo run $DB $NEWCOLL 1 --gnaf act --drop
cargo run $DB $NEWCOLL 2 --gnaf nsw
cargo run $DB $NEWCOLL 3 --gnaf nt
cargo run $DB $NEWCOLL 4 --gnaf ot
cargo run $DB $NEWCOLL 5 --gnaf qld
cargo run $DB $NEWCOLL 6 --gnaf sa
cargo run $DB $NEWCOLL 7 --gnaf tas
cargo run $DB $NEWCOLL 8 --gnaf vic
cargo run $DB $NEWCOLL 9 --gnaf wa
