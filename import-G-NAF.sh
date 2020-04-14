#!/bin/bash
#
# change the path below to where you unzipped your downloaded data.

if [ "$#" -ne 1 ]; then
	echo "usage $0 dbname"
	exit 1
fi

DB=$1

pushd PATH_TO_UNZIPPED_DATA/G-NAF/G-NAF\ FEBRUARY\ 2020/Standard
cat ACT_ADDRESS_DETAIL_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c act_address_detail --headerline --drop
cat ACT_STREET_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c act_street_locality --headerline --drop
cat ACT_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c act_locality --headerline --drop
cat ACT_ADDRESS_DEFAULT_GEOCODE_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c act_address_default_geocode --headerline --drop
cat NSW_ADDRESS_DETAIL_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c nsw_address_detail --headerline --drop
cat NSW_STREET_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c nsw_street_locality --headerline --drop
cat NSW_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c nsw_locality --headerline --drop
cat NSW_ADDRESS_DEFAULT_GEOCODE_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c nsw_address_default_geocode --headerline --drop
cat NT_ADDRESS_DETAIL_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c nt_address_detail --headerline --drop
cat NT_STREET_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c nt_street_locality --headerline --drop
cat NT_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c nt_locality --headerline --drop
cat NT_ADDRESS_DEFAULT_GEOCODE_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c nt_address_default_geocode --headerline --drop
cat OT_ADDRESS_DETAIL_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c ot_address_detail --headerline --drop
cat OT_STREET_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c ot_street_locality --headerline --drop
cat OT_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c ot_locality --headerline --drop
cat OT_ADDRESS_DEFAULT_GEOCODE_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c ot_address_default_geocode --headerline --drop
cat QLD_ADDRESS_DETAIL_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c qld_address_detail --headerline --drop
cat QLD_STREET_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c qld_street_locality --headerline --drop
cat QLD_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c qld_locality --headerline --drop
cat QLD_ADDRESS_DEFAULT_GEOCODE_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c qld_address_default_geocode --headerline --drop
cat SA_ADDRESS_DETAIL_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c sa_address_detail --headerline --drop
cat SA_STREET_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c sa_street_locality --headerline --drop
cat SA_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c sa_locality --headerline --drop
cat SA_ADDRESS_DEFAULT_GEOCODE_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c sa_address_default_geocode --headerline --drop
cat TAS_ADDRESS_DETAIL_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c tas_address_detail --headerline --drop
cat TAS_STREET_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c tas_street_locality --headerline --drop
cat TAS_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c tas_locality --headerline --drop
cat TAS_ADDRESS_DEFAULT_GEOCODE_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c tas_address_default_geocode --headerline --drop
cat VIC_ADDRESS_DETAIL_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c vic_address_detail --headerline --drop
cat VIC_STREET_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c vic_street_locality --headerline --drop
cat VIC_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c vic_locality --headerline --drop
cat VIC_ADDRESS_DEFAULT_GEOCODE_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c vic_address_default_geocode --headerline --drop
cat WA_ADDRESS_DETAIL_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c wa_address_detail --headerline --drop
cat WA_STREET_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c wa_street_locality --headerline --drop
cat WA_LOCALITY_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c wa_locality --headerline --drop
cat WA_ADDRESS_DEFAULT_GEOCODE_psv.psv | sed 's/^/"/;s/|/","/g;s/$/"/' | mongoimport --type csv -d $DB -c wa_address_default_geocode --headerline --drop
popd

# create all the indexes.
mongo <<EOF
use $DB
db.runCommand(
  { createIndexes: "act_street_locality", indexes: [ { key: { "STREET_LOCALITY_PID": 1 }, name: "STREET_LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "act_locality", indexes: [ { key: { "LOCALITY_PID": 1 }, name: "LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "act_address_default_geocode", indexes: [ { key: { "ADDRESS_DETAIL_PID": 1 }, name: "ADDRESS_DETAIL_PID" } ] }
)
db.runCommand(
  { createIndexes: "nsw_street_locality", indexes: [ { key: { "STREET_LOCALITY_PID": 1 }, name: "STREET_LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "nsw_locality", indexes: [ { key: { "LOCALITY_PID": 1 }, name: "LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "nsw_address_default_geocode", indexes: [ { key: { "ADDRESS_DETAIL_PID": 1 }, name: "ADDRESS_DETAIL_PID" } ] }
)
db.runCommand(
  { createIndexes: "nt_street_locality", indexes: [ { key: { "STREET_LOCALITY_PID": 1 }, name: "STREET_LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "nt_locality", indexes: [ { key: { "LOCALITY_PID": 1 }, name: "LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "nt_address_default_geocode", indexes: [ { key: { "ADDRESS_DETAIL_PID": 1 }, name: "ADDRESS_DETAIL_PID" } ] }
)
db.runCommand(
  { createIndexes: "ot_street_locality", indexes: [ { key: { "STREET_LOCALITY_PID": 1 }, name: "STREET_LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "ot_locality", indexes: [ { key: { "LOCALITY_PID": 1 }, name: "LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "ot_address_default_geocode", indexes: [ { key: { "ADDRESS_DETAIL_PID": 1 }, name: "ADDRESS_DETAIL_PID" } ] }
)
db.runCommand(
  { createIndexes: "qld_street_locality", indexes: [ { key: { "STREET_LOCALITY_PID": 1 }, name: "STREET_LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "qld_locality", indexes: [ { key: { "LOCALITY_PID": 1 }, name: "LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "qld_address_default_geocode", indexes: [ { key: { "ADDRESS_DETAIL_PID": 1 }, name: "ADDRESS_DETAIL_PID" } ] }
)
db.runCommand(
  { createIndexes: "sa_street_locality", indexes: [ { key: { "STREET_LOCALITY_PID": 1 }, name: "STREET_LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "sa_locality", indexes: [ { key: { "LOCALITY_PID": 1 }, name: "LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "sa_address_default_geocode", indexes: [ { key: { "ADDRESS_DETAIL_PID": 1 }, name: "ADDRESS_DETAIL_PID" } ] }
)
db.runCommand(
  { createIndexes: "tas_street_locality", indexes: [ { key: { "STREET_LOCALITY_PID": 1 }, name: "STREET_LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "tas_locality", indexes: [ { key: { "LOCALITY_PID": 1 }, name: "LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "tas_address_default_geocode", indexes: [ { key: { "ADDRESS_DETAIL_PID": 1 }, name: "ADDRESS_DETAIL_PID" } ] }
)
db.runCommand(
  { createIndexes: "vic_street_locality", indexes: [ { key: { "STREET_LOCALITY_PID": 1 }, name: "STREET_LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "vic_locality", indexes: [ { key: { "LOCALITY_PID": 1 }, name: "LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "vic_address_default_geocode", indexes: [ { key: { "ADDRESS_DETAIL_PID": 1 }, name: "ADDRESS_DETAIL_PID" } ] }
)
db.runCommand(
  { createIndexes: "wa_street_locality", indexes: [ { key: { "STREET_LOCALITY_PID": 1 }, name: "STREET_LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "wa_locality", indexes: [ { key: { "LOCALITY_PID": 1 }, name: "LOCALITY_PID" } ] }
)
db.runCommand(
  { createIndexes: "wa_address_default_geocode", indexes: [ { key: { "ADDRESS_DETAIL_PID": 1 }, name: "ADDRESS_DETAIL_PID" } ] }
)
EOF
