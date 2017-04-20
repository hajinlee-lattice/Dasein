#!/bin/bash

# call this as:
# ./setupdb_aws.sh testmigration-cluster.cluster-c6q8lwiagbkt.us-east-1.rds.amazonaws.com root 'Lattice123'

# this translates to:
# mysql -h lpi-cluster.cluster-ctigbumfbvzz.us-east-1.rds.amazonaws.com -u root -pLattice123

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

source $WSHOME/le-dev/scripts/setupdb_parameters.sh

. setupdb_pls_multitenant_migration.sh
. setupdb_oauth2_migration.sh
. setupdb_globalauth_migration.sh
