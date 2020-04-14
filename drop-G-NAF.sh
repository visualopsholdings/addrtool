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
db.act_address_detail.drop()
db.act_street_locality.drop()
db.act_locality.drop()
db.act_address_default_geocode.drop()
db.nsw_address_detail.drop()
db.nsw_street_locality.drop()
db.nsw_locality.drop()
db.nsw_address_default_geocode.drop()
db.nt_address_detail.drop()
db.nt_street_locality.drop()
db.nt_locality.drop()
db.nt_address_default_geocode.drop()
db.ot_address_detail.drop()
db.ot_street_locality.drop()
db.ot_locality.drop()
db.ot_address_default_geocode.drop()
db.qld_address_detail.drop()
db.qld_street_locality.drop()
db.qld_locality.drop()
db.qld_address_default_geocode.drop()
db.sa_address_detail.drop()
db.sa_street_locality.drop()
db.sa_locality.drop()
db.sa_address_default_geocode.drop()
db.tas_address_detail.drop()
db.tas_street_locality.drop()
db.tas_locality.drop()
db.tas_address_default_geocode.drop()
db.vic_address_detail.drop()
db.vic_street_locality.drop()
db.vic_locality.drop()
db.vic_address_default_geocode.drop()
db.wa_address_detail.drop()
db.wa_street_locality.drop()
db.wa_locality.drop()
db.wa_address_default_geocode.drop()
EOF

